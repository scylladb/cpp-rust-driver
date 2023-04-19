macro_rules! assert_cass_error_eq {
    ($expr:expr, $error:expr $(,)?) => {{
        use crate::argconv::ptr_to_cstr;
        use crate::external::cass_error_desc;
        let ___x = $expr;
        assert_eq!(
            ___x,
            $error,
            "expected \"{}\", instead got \"{}\"",
            ptr_to_cstr(cass_error_desc($error)).unwrap(),
            ptr_to_cstr(cass_error_desc(___x)).unwrap()
        );
    }};
}
pub(crate) use assert_cass_error_eq;
