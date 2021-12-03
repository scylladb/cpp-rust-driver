use crate::cass_types::CassDataType;
use scylla::frame::response::result::CqlValue;

pub fn is_compatible_type(_data_type: &CassDataType, _value: &Option<CqlValue>) -> bool {
    // TODO: cppdriver actually checks types.
    true
}

macro_rules! make_index_binder {
    ($this:ty, $consume_v:expr, $fn_by_idx:ident, $e:expr, [$($arg:ident @ $t:ty), *]) => {
        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_by_idx(
            this: *mut $this,
            index: size_t,
            $($arg: $t), *
        ) -> CassError {
            // For some reason detected as unused, which is not true
            #[allow(unused_imports)]
            use scylla::frame::response::result::CqlValue::*;
            match ($e)($($arg), *) {
                Ok(v) => $consume_v(ptr_to_ref_mut(this), index as usize, v),
                Err(e) => e,
            }
        }
    }
}

macro_rules! make_name_binder {
    ($this:ty, $consume_v:expr, $fn_by_name:ident, $e:expr, [$($arg:ident @ $t:ty), *]) => {
        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_by_name(
            this: *mut $this,
            name: *const c_char,
            $($arg: $t), *
        ) -> CassError {
            // For some reason detected as unused, which is not true
            #[allow(unused_imports)]
            use scylla::frame::response::result::CqlValue::*;
            let name = ptr_to_cstr(name).unwrap();
            match ($e)($($arg), *) {
                Ok(v) => $consume_v(ptr_to_ref_mut(this), name, v),
                Err(e) => e,
            }
        }
    }
}

macro_rules! make_name_n_binder {
    ($this:ty, $consume_v:expr, $fn_by_name_n:ident, $e:expr, [$($arg:ident @ $t:ty), *]) => {
        #[no_mangle]
        #[allow(clippy::redundant_closure_call)]
        pub unsafe extern "C" fn $fn_by_name_n(
            this: *mut $this,
            name: *const c_char,
            name_length: size_t,
            $($arg: $t), *
        ) -> CassError {
            // For some reason detected as unused, which is not true
            #[allow(unused_imports)]
            use scylla::frame::response::result::CqlValue::*;
            let name = ptr_to_cstr_n(name, name_length).unwrap();
            match ($e)($($arg), *) {
                Ok(v) => $consume_v(ptr_to_ref_mut(this), name, v),
                Err(e) => e,
            }
        }
    }
}

macro_rules! binders_maker_types {
    (null, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!($this, $consume_v, $fn, || Ok(None), []);
    };
    (int8, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(TinyInt(v))),
            [v @ cass_int8_t]
        );
    };
    (int16, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(SmallInt(v))),
            [v @ cass_int16_t]
        );
    };
    (int32, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(Int(v))),
            [v @ cass_int32_t]
        );
    };
    (uint32, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(Date(v))),
            [v @ cass_uint32_t]
        );
    };
    (int64, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(BigInt(v))),
            [v @ cass_int64_t]
        );
    };
    (float, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(Float(v))),
            [v @ cass_float_t]
        );
    };
    (double, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(Double(v))),
            [v @ cass_double_t]
        );
    };
    (bool, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(Boolean(v != 0))),
            [v @ cass_bool_t]
        );
    };
    (string, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v| Ok(Some(Text(ptr_to_cstr(v).unwrap().to_string()))),
            [v @ *const std::os::raw::c_char]
        );
    };
    (string_n, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v, n| Ok(Some(Text(ptr_to_cstr_n(v, n).unwrap().to_string()))),
            [v @ *const std::os::raw::c_char, n @ size_t]
        );
    };
    (bytes, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v, v_size| {
                let v_vec = std::slice::from_raw_parts(v, v_size as usize).to_vec();
                Ok(Some(Blob(v_vec)))
            },
            [v @ *const cass_byte_t, v_size @ size_t]
        );
    };
    (uuid, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v: crate::uuid::CassUuid| Ok(Some(Uuid(v.into()))),
            [v @ crate::uuid::CassUuid]
        );
    };
    (inet, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |v: crate::inet::CassInet| {
                // Err if length in struct is invalid.
                // cppdriver doesn't check this - it encodes any length given to it
                // but it doesn't seem like something we wanna do. Also, rust driver can't
                // really do it afaik.
                match std::convert::TryInto::try_into(v) {
                    Ok(v) => Ok(Some(Inet(v))),
                    Err(_) => Err(CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE),
                }
            },
            [v @ crate::inet::CassInet]
        );
    };
    (collection, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |p: *const crate::collection::CassCollection| {
                match std::convert::TryInto::try_into(ptr_to_ref(p).clone()) {
                    Ok(v) => Ok(Some(v)),
                    Err(_) => Err(CassError::CASS_ERROR_LIB_INVALID_VALUE_TYPE),
                }
            },
            [p @ *const crate::collection::CassCollection]
        );
    };
    (user_type, $macro_name:ident, $this:ty, $consume_v:expr, $fn:ident) => {
        $macro_name!(
            $this,
            $consume_v,
            $fn,
            |p: *const crate::user_type::CassUserType| Ok(Some(ptr_to_ref(p).into())),
            [p @ *const crate::user_type::CassUserType]
        );
    };
}

macro_rules! prepare_binders_macro {
    (@only_index $this:ty, $consume_v:expr) => {
        macro_rules! make_binders {
            ($t:ident, $fn:ident) => {
                binders_maker_types!($t, make_index_binder, $this, $consume_v, $fn);
            };
        }
    };
    (@index_and_name $this:ty, $consume_v_idx:expr, $consume_v_name:expr) => {
        macro_rules! make_binders {
            ($t:ident, $fn_idx:ident, $fn_name:ident, $fn_name_n:ident) => {
                binders_maker_types!($t, make_index_binder, $this, $consume_v_idx, $fn_idx);
                binders_maker_types!($t, make_name_binder, $this, $consume_v_name, $fn_name);
                binders_maker_types!($t, make_name_n_binder, $this, $consume_v_name, $fn_name_n);
            };
            ($t_idx:ident, $fn_idx:ident, $t_name:ident, $fn_name:ident, $t_name_n:ident, $fn_name_n:ident) => {
                binders_maker_types!($t_idx, make_index_binder, $this, $consume_v_idx, $fn_idx);
                binders_maker_types!($t_name, make_name_binder, $this, $consume_v_name, $fn_name);
                binders_maker_types!($t_name_n, make_name_n_binder, $this, $consume_v_name, $fn_name_n);
            };
            (@index $t:ident, $fn_idx:ident) => {
                binders_maker_types!($t, make_index_binder, $this, $consume_v_idx, $fn_idx);
            };
            (@name $t:ident, $fn_name:ident) => {
                binders_maker_types!($t, make_name_binder, $this, $consume_v_name, $fn_name);
            };
            (@name_n $t:ident, $fn_name_n:ident) => {
                binders_maker_types!($t, make_name_n_binder, $this, $consume_v_name, $fn_name_n);
            };
        }
    };
}
