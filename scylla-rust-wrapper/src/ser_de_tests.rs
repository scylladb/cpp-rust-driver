//! This module introduced serialization-deserialization unit tests.
//!
//! ### Safety
//! All of the `unsafe` blocks in this module can be justified using
//! the same reasoning: In each case, the `value` pointer is non-null
//! and derived from a valid reference to the `CassValue` object
//! that is alive and valid for the duration of the `unsafe` block.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ffi::c_char;
use std::net::{IpAddr, Ipv6Addr};
use std::ptr::addr_of_mut;
use std::sync::Arc;

use bytes::Bytes;
use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType, UserDefinedType};
use scylla::deserialize::value::DeserializeValue;
use scylla::deserialize::FrameSlice;
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::CellWriter;
use scylla::value::{CqlDecimal, CqlDuration, CqlValue};
use uuid::Uuid;

use crate::argconv::{
    CConst, CMut, CassBorrowedExclusivePtr, CassBorrowedSharedPtr, CassOwnedExclusivePtr, RefFFI,
};
use crate::cass_error::CassError;
use crate::cass_types::get_column_type;
use crate::inet::CassInet;
use crate::iterator::{
    cass_iterator_fields_from_user_type, cass_iterator_free, cass_iterator_from_collection,
    cass_iterator_from_map, cass_iterator_from_tuple, cass_iterator_get_map_key,
    cass_iterator_get_map_value, cass_iterator_get_user_type_field_name,
    cass_iterator_get_user_type_field_value, cass_iterator_get_value, cass_iterator_next,
    cass_iterator_type, CassIterator, CassIteratorType,
};
use crate::query_result::cass_raw_value::CassRawValue;
use crate::query_result::{
    cass_value_get_bool, cass_value_get_bytes, cass_value_get_decimal, cass_value_get_double,
    cass_value_get_duration, cass_value_get_float, cass_value_get_inet, cass_value_get_int16,
    cass_value_get_int32, cass_value_get_int64, cass_value_get_int8, cass_value_get_string,
    cass_value_get_uuid, cass_value_is_null, cass_value_item_count, CassValue,
};
use crate::testing::{assert_cass_error_eq, setup_tracing};
use crate::types::size_t;
use crate::uuid::CassUuid;

fn do_serialize<T: SerializeValue>(t: T, typ: &ColumnType) -> Vec<u8> {
    let mut ret = Vec::new();
    let writer = CellWriter::new(&mut ret);
    t.serialize(typ, writer).map(|_| ()).map(|()| ret).unwrap()
}

fn do_deserialize<'frame, 'metadata, T>(
    typ: &'metadata ColumnType<'metadata>,
    bytes: &'frame Bytes,
) -> T
where
    T: DeserializeValue<'frame, 'metadata>,
{
    <T as DeserializeValue<'frame, 'metadata>>::type_check(typ).unwrap();
    let mut frame_slice = FrameSlice::new(bytes);
    let value = frame_slice.read_cql_bytes().unwrap();
    <T as DeserializeValue<'frame, 'metadata>>::deserialize(typ, value).unwrap()
}

/// Given a pointer to a `CassValue`, this trait allows you to convert it to a Rust type.
trait FromCassValuePtr {
    fn from_cass_value_ptr(value: CassBorrowedSharedPtr<CassValue, CConst>) -> Self;
}

macro_rules! impl_from_cass_value_ptr_for_fixed_numeric {
    ($cass_getter:ident) => {
        fn from_cass_value_ptr(value: CassBorrowedSharedPtr<CassValue, CConst>) -> Self {
            let mut output = Default::default();
            unsafe {
                assert_cass_error_eq!(
                    $cass_getter(value, addr_of_mut!(output)),
                    CassError::CASS_OK
                );
            }
            output
        }
    };
}

impl FromCassValuePtr for i8 {
    impl_from_cass_value_ptr_for_fixed_numeric!(cass_value_get_int8);
}
impl FromCassValuePtr for i16 {
    impl_from_cass_value_ptr_for_fixed_numeric!(cass_value_get_int16);
}
impl FromCassValuePtr for i32 {
    impl_from_cass_value_ptr_for_fixed_numeric!(cass_value_get_int32);
}
impl FromCassValuePtr for i64 {
    impl_from_cass_value_ptr_for_fixed_numeric!(cass_value_get_int64);
}
impl FromCassValuePtr for f32 {
    impl_from_cass_value_ptr_for_fixed_numeric!(cass_value_get_float);
}
impl FromCassValuePtr for f64 {
    impl_from_cass_value_ptr_for_fixed_numeric!(cass_value_get_double);
}
impl FromCassValuePtr for bool {
    fn from_cass_value_ptr(value: CassBorrowedSharedPtr<CassValue, CConst>) -> Self {
        let mut output: u32 = 0;
        unsafe {
            assert_cass_error_eq!(
                cass_value_get_bool(value, addr_of_mut!(output)),
                CassError::CASS_OK
            );
        }
        output > 0
    }
}
impl FromCassValuePtr for Vec<u8> {
    fn from_cass_value_ptr(value: CassBorrowedSharedPtr<CassValue, CConst>) -> Self {
        let mut output: *const u8 = std::ptr::null();
        let mut output_size: size_t = 0;
        unsafe {
            assert_cass_error_eq!(
                cass_value_get_bytes(value, addr_of_mut!(output), addr_of_mut!(output_size)),
                CassError::CASS_OK
            );
            std::slice::from_raw_parts(output, output_size as usize).to_vec()
        }
    }
}
impl FromCassValuePtr for String {
    fn from_cass_value_ptr(value: CassBorrowedSharedPtr<CassValue, CConst>) -> Self {
        let mut output: *const c_char = std::ptr::null();
        let mut output_size: size_t = 0;
        unsafe {
            assert_cass_error_eq!(
                cass_value_get_string(value, addr_of_mut!(output), addr_of_mut!(output_size)),
                CassError::CASS_OK
            );
            std::str::from_utf8(std::slice::from_raw_parts(
                output as *const u8,
                output_size as usize,
            ))
            .unwrap()
            .to_owned()
        }
    }
}
impl FromCassValuePtr for Uuid {
    fn from_cass_value_ptr(value: CassBorrowedSharedPtr<CassValue, CConst>) -> Self {
        let mut output: CassUuid = CassUuid {
            time_and_version: 0,
            clock_seq_and_node: 0,
        };
        unsafe {
            assert_cass_error_eq!(
                cass_value_get_uuid(value, addr_of_mut!(output)),
                CassError::CASS_OK
            );
        }
        output.into()
    }
}
impl FromCassValuePtr for IpAddr {
    fn from_cass_value_ptr(value: CassBorrowedSharedPtr<CassValue, CConst>) -> Self {
        let mut output: CassInet = CassInet {
            address: [0; 16],
            address_length: 0,
        };
        unsafe {
            assert_cass_error_eq!(
                cass_value_get_inet(value, addr_of_mut!(output)),
                CassError::CASS_OK
            );
        }
        let bytes = &output.address[..output.address_length as usize];
        IpAddr::from(Ipv6Addr::from_bits(u128::from_be_bytes(
            bytes.try_into().unwrap(),
        )))
    }
}
impl FromCassValuePtr for CqlDecimal {
    fn from_cass_value_ptr(value: CassBorrowedSharedPtr<CassValue, CConst>) -> Self {
        let mut varint: *const u8 = std::ptr::null();
        let mut varint_size: size_t = 0;
        let mut scale: i32 = 0;
        unsafe {
            assert_cass_error_eq!(
                cass_value_get_decimal(
                    value,
                    addr_of_mut!(varint),
                    addr_of_mut!(varint_size),
                    addr_of_mut!(scale)
                ),
                CassError::CASS_OK
            );
        }
        let varint = unsafe { std::slice::from_raw_parts(varint, varint_size as usize) };
        CqlDecimal::from_signed_be_bytes_slice_and_exponent(varint, scale)
    }
}
impl FromCassValuePtr for CqlDuration {
    fn from_cass_value_ptr(value: CassBorrowedSharedPtr<CassValue, CConst>) -> Self {
        let mut months: i32 = 0;
        let mut days: i32 = 0;
        let mut nanoseconds: i64 = 0;
        unsafe {
            assert_cass_error_eq!(
                cass_value_get_duration(
                    value,
                    addr_of_mut!(months),
                    addr_of_mut!(days),
                    addr_of_mut!(nanoseconds)
                ),
                CassError::CASS_OK
            );
        }
        CqlDuration {
            months,
            days,
            nanoseconds,
        }
    }
}

/// Serializes the `to_serialize_non_null` value to bytes.
/// Then, the bytes are deserialized to a `CassValue` object.
/// The `CassValue` object is converted back to the original type using `cass_value_ptr_to_rust_type`.
/// Finally, the original value `to_serialize_non_null` is compared with the deserialized value.
/// The serialized bytes are also compared with the bytes obtained from `cass_value_get_bytes` (only if the value is non-null).
fn test_deserialize<T, F>(typ: ColumnType, to_serialize_non_null: T, cass_value_ptr_to_rust_type: F)
where
    T: SerializeValue + PartialEq + std::fmt::Debug,
    F: Fn(CassBorrowedSharedPtr<CassValue, CConst>) -> T,
{
    let bytes = Bytes::from(do_serialize(&to_serialize_non_null, &typ));
    let data_type = Arc::new(get_column_type(&typ));
    let cass_value = CassValue {
        value: do_deserialize::<CassRawValue>(&typ, &bytes),
        value_type: &data_type,
    };
    let value_ptr = RefFFI::as_ptr(&cass_value);

    // Check that we retrieved the original value.
    let actual = cass_value_ptr_to_rust_type(value_ptr.borrow());
    assert_eq!(to_serialize_non_null, actual);

    if unsafe { cass_value_is_null(value_ptr.borrow()) == 0 } {
        // Compare the serialized bytes with the bytes from `cass_value_get_bytes`.
        let actual_bytes = unsafe {
            let mut output: *const u8 = std::ptr::null();
            let mut output_size: size_t = 0;
            assert_cass_error_eq!(
                cass_value_get_bytes(value_ptr, addr_of_mut!(output), addr_of_mut!(output_size)),
                CassError::CASS_OK
            );

            std::slice::from_raw_parts(output, output_size as usize).to_vec()
        };
        // Skip the first 4 bytes, which are the length of the serialized data.
        let expected_bytes = &bytes.slice(4..);
        assert_eq!(actual_bytes.as_slice(), expected_bytes);
    }
}

#[test]
fn test_deserialize_value_native() {
    setup_tracing();

    tracing::info!("Testing bool...");
    test_deserialize(
        ColumnType::Native(NativeType::Boolean),
        true,
        bool::from_cass_value_ptr,
    );

    tracing::info!("Testing int8...");
    test_deserialize(
        ColumnType::Native(NativeType::TinyInt),
        42_i8,
        i8::from_cass_value_ptr,
    );

    tracing::info!("Testing int16...");
    test_deserialize(
        ColumnType::Native(NativeType::SmallInt),
        4242_i16,
        i16::from_cass_value_ptr,
    );

    tracing::info!("Testing int32...");
    test_deserialize(
        ColumnType::Native(NativeType::Int),
        424242_i32,
        i32::from_cass_value_ptr,
    );

    tracing::info!("Testing int64...");
    test_deserialize(
        ColumnType::Native(NativeType::BigInt),
        42424242_i64,
        i64::from_cass_value_ptr,
    );

    tracing::info!("Testing float...");
    test_deserialize(
        ColumnType::Native(NativeType::Float),
        42.42_f32,
        f32::from_cass_value_ptr,
    );

    tracing::info!("Testing double...");
    test_deserialize(
        ColumnType::Native(NativeType::Double),
        4242.4242_f64,
        f64::from_cass_value_ptr,
    );

    tracing::info!("Testing bytes...");
    test_deserialize::<Vec<u8>, _>(
        ColumnType::Native(NativeType::Blob),
        vec![0x42, 0x21, 0x37, 0x00],
        Vec::<u8>::from_cass_value_ptr,
    );

    tracing::info!("Testing uuid...");
    test_deserialize(
        ColumnType::Native(NativeType::Uuid),
        Uuid::from_slice(&[
            0x8e, 0x14, 0xe7, 0x60, 0x7f, 0xa8, 0x11, 0xeb, 0xbc, 0x66, 0, 0, 0, 0, 0, 0x01,
        ])
        .unwrap(),
        Uuid::from_cass_value_ptr,
    );

    tracing::info!("Testing inet...");
    test_deserialize(
        ColumnType::Native(NativeType::Inet),
        IpAddr::V6(Ipv6Addr::new(1, 2, 3, 4, 5, 6, 7, 8)),
        IpAddr::from_cass_value_ptr,
    );

    tracing::info!("Testing decimal...");
    test_deserialize(
        ColumnType::Native(NativeType::Decimal),
        CqlDecimal::from_signed_be_bytes_slice_and_exponent(b"Ala ma kota", 42),
        CqlDecimal::from_cass_value_ptr,
    );

    tracing::info!("Testing string...");
    test_deserialize(
        ColumnType::Native(NativeType::Text),
        String::from("Ala ma kota, a kot ma psa"),
        String::from_cass_value_ptr,
    );

    tracing::info!("Testing duration...");
    test_deserialize(
        ColumnType::Native(NativeType::Duration),
        CqlDuration {
            months: 6,
            days: 25,
            nanoseconds: 213742,
        },
        CqlDuration::from_cass_value_ptr,
    );

    tracing::info!("Testing null...");
    test_deserialize::<Option<bool>, _>(
        ColumnType::Native(NativeType::Boolean),
        None,
        |value_ptr| unsafe { (cass_value_is_null(value_ptr) == 0).then_some(true) },
    );

    tracing::info!("Testing empty...");
    test_deserialize::<CqlValue, _>(
        ColumnType::Native(NativeType::Boolean),
        CqlValue::Empty,
        |value_ptr| unsafe {
            if cass_value_is_null(value_ptr) != 0 {
                CqlValue::Empty
            } else {
                CqlValue::Boolean(true)
            }
        },
    );
}

#[test]
fn test_deserialize_tuple_iterator() {
    let typ = ColumnType::Tuple(vec![
        ColumnType::Native(NativeType::Int),
        ColumnType::Native(NativeType::Text),
        ColumnType::Native(NativeType::Float),
    ]);
    let to_serialize = (42_i32, String::from("Ala ma kota"), 42.42_f32);
    let bytes = Bytes::from(do_serialize(&to_serialize, &typ));
    let data_type = Arc::new(get_column_type(&typ));
    let cass_value = CassValue {
        value: do_deserialize::<CassRawValue>(&typ, &bytes),
        value_type: &data_type,
    };
    let value_ptr = RefFFI::as_ptr(&cass_value);

    unsafe {
        assert_eq!(cass_value_item_count(value_ptr.borrow()), 3);

        let mut iter = cass_iterator_from_tuple(value_ptr);
        assert!(!iter.is_null());
        assert_eq!(
            cass_iterator_type(iter.borrow_mut()),
            CassIteratorType::CASS_ITERATOR_TYPE_TUPLE
        );

        let first_value: i32 = {
            assert!(cass_iterator_next(iter.borrow_mut()) > 0);
            let first_cass_value = cass_iterator_get_value(iter.borrow().into_c_const());
            i32::from_cass_value_ptr(first_cass_value)
        };
        assert_eq!(42, first_value);

        let second_value: String = {
            assert!(cass_iterator_next(iter.borrow_mut()) > 0);
            let second_cass_value = cass_iterator_get_value(iter.borrow().into_c_const());
            String::from_cass_value_ptr(second_cass_value)
        };
        assert_eq!("Ala ma kota", second_value);

        let third_value: f32 = {
            assert!(cass_iterator_next(iter.borrow_mut()) > 0);
            let third_cass_value = cass_iterator_get_value(iter.borrow().into_c_const());
            f32::from_cass_value_ptr(third_cass_value)
        };
        assert_eq!(42.42, third_value);

        // Iterator should be exhausted.
        assert!(cass_iterator_next(iter.borrow_mut()) == 0);

        cass_iterator_free(iter);
    }
}

#[test]
fn test_deserialize_list_iterator() {
    let typ = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Int))),
    };
    let to_serialize = Vec::<i32>::from([42, 4242, 424242, 42424242]);
    let bytes = Bytes::from(do_serialize(&to_serialize, &typ));
    let data_type = Arc::new(get_column_type(&typ));
    let cass_value = CassValue {
        value: do_deserialize::<CassRawValue>(&typ, &bytes),
        value_type: &data_type,
    };
    let value_ptr = RefFFI::as_ptr(&cass_value);

    unsafe {
        assert_eq!(cass_value_item_count(value_ptr.borrow()), 4);

        let mut iter = cass_iterator_from_collection(value_ptr);
        assert!(!iter.is_null());
        assert_eq!(
            cass_iterator_type(iter.borrow_mut()),
            CassIteratorType::CASS_ITERATOR_TYPE_COLLECTION
        );

        for v in to_serialize {
            let deserialized_value: i32 = {
                assert!(cass_iterator_next(iter.borrow_mut()) > 0);
                let cass_value = cass_iterator_get_value(iter.borrow().into_c_const());
                i32::from_cass_value_ptr(cass_value)
            };
            assert_eq!(v, deserialized_value);
        }

        // Iterator should be exhausted.
        assert!(cass_iterator_next(iter.borrow_mut()) == 0);

        cass_iterator_free(iter);
    }
}

#[test]
fn test_deserialize_set_iterator() {
    let typ = ColumnType::Collection {
        frozen: false,
        typ: CollectionType::Set(Box::new(ColumnType::Native(NativeType::Int))),
    };
    let to_serialize = HashSet::<i32>::from([42, 4242, 424242, 42424242]);
    let bytes = Bytes::from(do_serialize(&to_serialize, &typ));
    let data_type = Arc::new(get_column_type(&typ));
    let cass_value = CassValue {
        value: do_deserialize::<CassRawValue>(&typ, &bytes),
        value_type: &data_type,
    };
    let value_ptr = RefFFI::as_ptr(&cass_value);

    unsafe {
        assert_eq!(cass_value_item_count(value_ptr.borrow()), 4);

        let mut iter = cass_iterator_from_collection(value_ptr);
        assert!(!iter.is_null());
        assert_eq!(
            cass_iterator_type(iter.borrow_mut()),
            CassIteratorType::CASS_ITERATOR_TYPE_COLLECTION
        );

        let deserialized_set = std::iter::from_fn(|| {
            if cass_iterator_next(iter.borrow_mut()) == 0 {
                return None;
            };

            let cass_value = cass_iterator_get_value(iter.borrow().into_c_const());

            Some(i32::from_cass_value_ptr(cass_value))
        })
        .collect::<HashSet<_>>();

        assert_eq!(to_serialize, deserialized_set);

        // Iterator should be exhausted.
        assert!(cass_iterator_next(iter.borrow_mut()) == 0);

        cass_iterator_free(iter);
    }
}

#[test]
fn test_deserialize_map_iterator() {
    fn test_map_iterator_helper<I, F>(
        create_iter: I,
        expected_type: CassIteratorType,
        collect_to_map: F,
    ) where
        for<'a> I: Fn(
            CassBorrowedSharedPtr<'a, CassValue<'a>, CConst>,
        ) -> CassOwnedExclusivePtr<CassIterator<'a>, CMut>,
        F: Fn(CassBorrowedExclusivePtr<CassIterator, CMut>) -> HashMap<String, i32>,
    {
        let typ = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(
                Box::new(ColumnType::Native(NativeType::Text)),
                Box::new(ColumnType::Native(NativeType::Int)),
            ),
        };
        let to_serialize = HashMap::<String, i32>::from([
        (String::from("forty two"), 42),
        (String::from("four thousand two hundred forty-two"), 4242),
        (String::from("four hundred twenty-four thousand four hundred twenty-four"), 424242),
        (String::from("four hundred twenty-four million two hundred forty-two thousand four hundred twenty-two"), 42424242),
    ]);
        let bytes = Bytes::from(do_serialize(&to_serialize, &typ));
        let data_type = Arc::new(get_column_type(&typ));
        let cass_value = CassValue {
            value: do_deserialize::<CassRawValue>(&typ, &bytes),
            value_type: &data_type,
        };
        let value_ptr = RefFFI::as_ptr(&cass_value);

        unsafe {
            assert_eq!(cass_value_item_count(value_ptr.borrow()), 4);

            let mut iter = create_iter(value_ptr);
            assert!(!iter.is_null());
            assert_eq!(cass_iterator_type(iter.borrow_mut()), expected_type,);

            let deserialized_map = collect_to_map(iter.borrow_mut());

            assert_eq!(to_serialize, deserialized_map);

            tracing::info!("Deserialized map: {:?}", deserialized_map);
            // Iterator should be exhausted.
            assert!(cass_iterator_next(iter.borrow_mut()) == 0);

            cass_iterator_free(iter);
        }
    }

    setup_tracing();

    // Test `cass_iterator_from_collection`.
    tracing::info!("Testing `cass_iterator_from_collection` for map...");
    test_map_iterator_helper(
        |map_value| unsafe { cass_iterator_from_collection(map_value) },
        CassIteratorType::CASS_ITERATOR_TYPE_COLLECTION,
        |mut iter| {
            std::iter::from_fn(|| unsafe {
                // Move to the key.
                if cass_iterator_next(iter.borrow_mut()) == 0 {
                    return None;
                };

                let cass_value_key = cass_iterator_get_value(iter.borrow().into_c_const());
                let key = String::from_cass_value_ptr(cass_value_key);

                // Move to the value.
                if cass_iterator_next(iter.borrow_mut()) == 0 {
                    return None;
                };

                let cass_value_value = cass_iterator_get_value(iter.borrow().into_c_const());
                let value = i32::from_cass_value_ptr(cass_value_value);

                Some((key, value))
            })
            .collect::<HashMap<_, _>>()
        },
    );

    // Test `cass_iterator_from_map`.
    tracing::info!("Testing `cass_iterator_from_map`...");
    test_map_iterator_helper(
        |map_value| unsafe { cass_iterator_from_map(map_value) },
        CassIteratorType::CASS_ITERATOR_TYPE_MAP,
        |mut iter| unsafe {
            std::iter::from_fn(|| {
                // Move to the next key-vaue pair.
                if cass_iterator_next(iter.borrow_mut()) == 0 {
                    return None;
                };

                let cass_value_key = cass_iterator_get_map_key(iter.borrow().into_c_const());
                let key = String::from_cass_value_ptr(cass_value_key);

                let cass_value_value = cass_iterator_get_map_value(iter.borrow().into_c_const());
                let value = i32::from_cass_value_ptr(cass_value_value);

                Some((key, value))
            })
            .collect::<HashMap<_, _>>()
        },
    );
}

#[test]
fn test_deserialize_udt_iterator() {
    fn udt_def_with_fields(
        fields: impl IntoIterator<Item = (impl Into<Cow<'static, str>>, ColumnType<'static>)>,
    ) -> ColumnType<'static> {
        ColumnType::UserDefinedType {
            frozen: false,
            definition: Arc::new(UserDefinedType {
                name: "udt".into(),
                keyspace: "ks".into(),
                field_types: fields.into_iter().map(|(s, t)| (s.into(), t)).collect(),
            }),
        }
    }
    fn get_field_name<'a>(iter: CassBorrowedSharedPtr<'a, CassIterator<'a>, CConst>) -> &'a str {
        let mut field_name: *const c_char = std::ptr::null();
        let mut field_name_length: size_t = 0;
        unsafe {
            assert_cass_error_eq!(
                cass_iterator_get_user_type_field_name(
                    iter,
                    addr_of_mut!(field_name),
                    addr_of_mut!(field_name_length)
                ),
                CassError::CASS_OK
            );
            std::str::from_utf8(std::slice::from_raw_parts(
                field_name as *const u8,
                field_name_length as usize,
            ))
            .unwrap()
        }
    }

    #[derive(scylla::SerializeValue)]
    struct MyUdt {
        field1: i32,
        field2: String,
        field3: f32,
    }

    let serialize_typ = udt_def_with_fields([
        ("field1", ColumnType::Native(NativeType::Int)),
        ("field2", ColumnType::Native(NativeType::Text)),
        ("field3", ColumnType::Native(NativeType::Float)),
    ]);
    let to_serialize = MyUdt {
        field1: 42,
        field2: String::from("Ala ma kota"),
        field3: 42.42_f32,
    };
    let bytes = Bytes::from(do_serialize(&to_serialize, &serialize_typ));

    // Add an extra field during deserialization to check that we properly handle missing values.
    let deserialize_typ = udt_def_with_fields([
        ("field1", ColumnType::Native(NativeType::Int)),
        ("field2", ColumnType::Native(NativeType::Text)),
        ("field3", ColumnType::Native(NativeType::Float)),
        ("field4", ColumnType::Native(NativeType::Boolean)), // Extra field
    ]);
    let data_type = Arc::new(get_column_type(&deserialize_typ));
    let cass_value = CassValue {
        value: do_deserialize::<CassRawValue>(&deserialize_typ, &bytes),
        value_type: &data_type,
    };
    let value_ptr = RefFFI::as_ptr(&cass_value);

    unsafe {
        // Item count should be 4, including the extra field in the type.
        assert_eq!(cass_value_item_count(value_ptr.borrow()), 4);

        let mut iter = cass_iterator_fields_from_user_type(value_ptr);
        assert!(!iter.is_null());

        assert_eq!(
            cass_iterator_type(iter.borrow_mut()),
            CassIteratorType::CASS_ITERATOR_TYPE_USER_TYPE_FIELD
        );

        let (field_name1, field_value1) = {
            assert!(cass_iterator_next(iter.borrow_mut()) > 0);
            let field_name = get_field_name(iter.borrow().into_c_const());
            let cass_value = cass_iterator_get_user_type_field_value(iter.borrow().into_c_const());
            let value = i32::from_cass_value_ptr(cass_value);
            (field_name, value)
        };
        assert_eq!("field1", field_name1);
        assert_eq!(42, field_value1);

        let (field_name2, field_value2) = {
            assert!(cass_iterator_next(iter.borrow_mut()) > 0);
            let field_name = get_field_name(iter.borrow().into_c_const());
            let cass_value = cass_iterator_get_user_type_field_value(iter.borrow().into_c_const());
            let field_value = String::from_cass_value_ptr(cass_value);
            (field_name, field_value)
        };
        assert_eq!("field2", field_name2);
        assert_eq!("Ala ma kota", field_value2);

        let (field_name3, field_value3) = {
            assert!(cass_iterator_next(iter.borrow_mut()) > 0);
            let field_name = get_field_name(iter.borrow().into_c_const());
            let cass_value = cass_iterator_get_user_type_field_value(iter.borrow().into_c_const());
            let field_value = f32::from_cass_value_ptr(cass_value);
            (field_name, field_value)
        };
        assert_eq!("field3", field_name3);
        assert_eq!(42.42, field_value3);

        let (field_name4, field_value4_is_null) = {
            assert!(cass_iterator_next(iter.borrow_mut()) > 0);
            let field_name = get_field_name(iter.borrow().into_c_const());
            let cass_value = cass_iterator_get_user_type_field_value(iter.borrow().into_c_const());
            let is_null = cass_value_is_null(cass_value) > 0;
            (field_name, is_null)
        };
        assert_eq!("field4", field_name4);
        assert!(field_value4_is_null);

        // Iterator should be exhausted.
        assert!(cass_iterator_next(iter.borrow_mut()) == 0);

        cass_iterator_free(iter);
    }
}
