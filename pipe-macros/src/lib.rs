use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, FnArg, ImplItem, ItemFn, ItemImpl, Pat, ReturnType, Type};

/// Derive an `Operator<A, B>` impl from a plain impl block.
///
/// The impl block must contain an `async fn execute(&self, input: A) -> Result<B, ...>`
/// method. The macro generates the trait impl with the correct `PinFut` return type.
///
/// The struct must derive `Debug` separately (required by the `Operator` trait).
///
/// ```ignore
/// #[derive(Debug)]
/// struct double;
///
/// #[operator]
/// impl Double {
///     async fn execute(&self, input: i64) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
///         Ok(input * 2)
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn operator(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);

    let struct_ty = &input.self_ty;
    let (impl_generics, _, where_clause) = input.generics.split_for_impl();

    let method = input
        .items
        .iter()
        .find_map(|item| match item {
            ImplItem::Fn(m) if m.sig.ident == "execute" => Some(m),
            _ => None,
        })
        .expect("#[operator] impl must contain an `execute` method");

    let (input_type, input_name) = extract_second_param(&method.sig);
    let output_type = extract_result_ok_type(&method.sig.output);
    let body = &method.block;

    let expanded = quote! {
        impl #impl_generics pipe::operator::Operator<#input_type, #output_type> for #struct_ty #where_clause {
            fn execute<'__op>(&'__op self, #input_name: #input_type)
                -> pipe::operator::PinFut<'__op, #output_type>
            {
                Box::pin(async move #body)
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive a `PullOperator<B>` impl from a plain impl block.
///
/// The impl block must contain an `async fn next_chunk(&mut self) -> Result<Option<Vec<B>>, PipeError>`
/// method. The macro generates the trait impl with the correct `ChunkFut` return type.
///
/// ```ignore
/// struct MyCursor { offset: usize }
///
/// #[pull_operator]
/// impl MyCursor {
///     async fn next_chunk(&mut self) -> Result<Option<Vec<i64>>, pipe::pull::PipeError> {
///         Ok(Some(vec![1, 2, 3]))
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn pull_operator(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);

    let struct_ty = &input.self_ty;
    let (impl_generics, _, where_clause) = input.generics.split_for_impl();

    let method = input
        .items
        .iter()
        .find_map(|item| match item {
            ImplItem::Fn(m) if m.sig.ident == "next_chunk" => Some(m),
            _ => None,
        })
        .expect("#[pull_operator] impl must contain a `next_chunk` method");

    let output_type = extract_chunk_element_type(&method.sig.output);
    let body = &method.block;

    let expanded = quote! {
        impl #impl_generics pipe::pull::PullOperator<#output_type> for #struct_ty #where_clause {
            fn next_chunk(&mut self) -> pipe::pull::ChunkFut<'_, #output_type> {
                Box::pin(async move #body)
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive an `Operator<A, B>` from a plain async function.
///
/// Generates a zero-sized struct with the function's name (PascalCase)
/// and implements `Operator<A, B>` on it. No struct or impl block needed.
///
/// The return type can be either a bare value (auto-wrapped in `Ok`) or
/// an explicit `Result`:
///
/// ```ignore
/// // Simple -- just return the value
/// #[pipe_fn]
/// async fn double(x: i64) -> i64 { x * 2 }
///
/// // Fallible -- return Result explicitly
/// #[pipe_fn]
/// async fn parse(s: String) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
///     Ok(s.parse()?)
/// }
///
/// let result = pipe![1, 2, 3].pipe(Double).collect().await?;
/// ```
#[proc_macro_attribute]
pub fn pipe_fn(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);

    let fn_name = &func.sig.ident;
    let struct_name = syn::Ident::new(
        &to_pascal_case(&fn_name.to_string()),
        fn_name.span(),
    );

    let param = func
        .sig
        .inputs
        .first()
        .expect("#[pipe_fn] function must have one parameter");
    let (input_type, input_name) = match param {
        FnArg::Typed(pat_type) => (&*pat_type.ty, &*pat_type.pat),
        _ => panic!("#[pipe_fn] parameter must be typed"),
    };

    let body = &func.block;

    // Check if return type is Result<B, ...> or a bare type B
    let (output_type, wrap_ok) = match try_extract_result_ok_type(&func.sig.output) {
        Some(ty) => (quote! { #ty }, false),
        None => {
            let ty = match &func.sig.output {
                ReturnType::Type(_, ty) => ty,
                ReturnType::Default => panic!("#[pipe_fn] must have a return type"),
            };
            (quote! { #ty }, true)
        }
    };

    let body_expr = if wrap_ok {
        quote! { Box::pin(async move { Ok((|| #body)()) }) }
    } else {
        quote! { Box::pin(async move #body) }
    };

    let expanded = quote! {
        #[derive(Debug)]
        struct #struct_name;

        impl pipe::operator::Operator<#input_type, #output_type> for #struct_name {
            fn execute<'__op>(&'__op self, #input_name: #input_type)
                -> pipe::operator::PinFut<'__op, #output_type>
            {
                #body_expr
            }
        }
    };

    TokenStream::from(expanded)
}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn extract_second_param(sig: &syn::Signature) -> (&Type, &Pat) {
    let arg = sig
        .inputs
        .iter()
        .nth(1)
        .expect("#[operator] execute must have a second parameter (the input)");

    match arg {
        FnArg::Typed(pat_type) => (&pat_type.ty, &pat_type.pat),
        _ => panic!("#[operator] execute second parameter must be typed"),
    }
}

fn try_extract_result_ok_type(ret: &ReturnType) -> Option<&Type> {
    let ty = match ret {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => return None,
    };
    if let Type::Path(type_path) = ty {
        let last = type_path.path.segments.last()?;
        if last.ident == "Result" {
            if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                if let Some(syn::GenericArgument::Type(ok_ty)) = args.args.first() {
                    return Some(ok_ty);
                }
            }
        }
    }
    None
}

fn extract_result_ok_type(ret: &ReturnType) -> &Type {
    let ty = match ret {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => panic!("#[operator] execute must have a return type"),
    };

    // Expect Result<B, ...> -- extract B
    if let Type::Path(type_path) = ty {
        let last = type_path.path.segments.last().expect("empty return type path");
        if last.ident == "Result" {
            if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                if let Some(syn::GenericArgument::Type(ok_ty)) = args.args.first() {
                    return ok_ty;
                }
            }
        }
    }

    panic!("#[operator] execute return type must be Result<B, ...>");
}

fn extract_chunk_element_type(ret: &ReturnType) -> &Type {
    let ty = match ret {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => panic!("#[pull_operator] next_chunk must have a return type"),
    };

    // Expect Result<Option<Vec<B>>, ...> -- extract B
    if let Type::Path(type_path) = ty {
        let last = type_path.path.segments.last().unwrap();
        if last.ident == "Result" {
            if let syn::PathArguments::AngleBracketed(args) = &last.arguments {
                if let Some(syn::GenericArgument::Type(ok_ty)) = args.args.first() {
                    // ok_ty should be Option<Vec<B>>
                    if let Type::Path(opt_path) = ok_ty {
                        let opt_seg = opt_path.path.segments.last().unwrap();
                        if opt_seg.ident == "Option" {
                            if let syn::PathArguments::AngleBracketed(opt_args) = &opt_seg.arguments
                            {
                                if let Some(syn::GenericArgument::Type(vec_ty)) =
                                    opt_args.args.first()
                                {
                                    // vec_ty should be Vec<B>
                                    if let Type::Path(vec_path) = vec_ty {
                                        let vec_seg = vec_path.path.segments.last().unwrap();
                                        if vec_seg.ident == "Vec" {
                                            if let syn::PathArguments::AngleBracketed(vec_args) =
                                                &vec_seg.arguments
                                            {
                                                if let Some(syn::GenericArgument::Type(elem_ty)) =
                                                    vec_args.args.first()
                                                {
                                                    return elem_ty;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    panic!("#[pull_operator] next_chunk return type must be Result<Option<Vec<B>>, PipeError>");
}
