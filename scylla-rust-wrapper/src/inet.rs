use crate::argconv::*;
use crate::cass_error::CassError;
use crate::types::*;
use std::convert::TryInto;
use std::net::IpAddr;
use std::os::raw::c_char;
use std::slice::from_raw_parts;
use std::str::FromStr;

#[repr(u8)] // address_length field in CassInet is cass_uint8_t
#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone)]
pub enum CassInetLength {
    CASS_INET_V4 = 4,
    CASS_INET_V6 = 16,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct CassInet {
    pub address: [cass_uint8_t; 16],
    pub address_length: CassInetLength,
}

unsafe fn cass_inet_init(address: *const cass_uint8_t, address_length: CassInetLength) -> CassInet {
    let mut array = [0; 16];
    let length = address_length as usize;
    array[0..length].clone_from_slice(from_raw_parts(address, length));

    CassInet {
        address: array,
        address_length,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cass_inet_init_v4(address: *const cass_uint8_t) -> CassInet {
    cass_inet_init(address, CassInetLength::CASS_INET_V4)
}

#[no_mangle]
pub unsafe extern "C" fn cass_inet_init_v6(address: *const cass_uint8_t) -> CassInet {
    cass_inet_init(address, CassInetLength::CASS_INET_V6)
}

#[no_mangle]
pub unsafe extern "C" fn cass_inet_string(inet: CassInet, output: *mut c_char) {
    let ip_addr: IpAddr = inet.into();

    let string_representation = ip_addr.to_string();
    std::ptr::copy_nonoverlapping(
        string_representation.as_ptr(),
        output as *mut u8,
        string_representation.len(),
    );

    // Null-terminate
    let null_byte = output.add(string_representation.len()) as *mut c_char;
    *null_byte = 0;
}

#[no_mangle]
pub unsafe extern "C" fn cass_inet_from_string(
    input: *const c_char,
    inet: *mut CassInet,
) -> CassError {
    let input_str = ptr_to_cstr(input).unwrap();
    let input_length = input_str.len();

    cass_inet_from_string_n(input, input_length as size_t, inet)
}

#[no_mangle]
pub unsafe extern "C" fn cass_inet_from_string_n(
    input_raw: *const c_char,
    input_length: size_t,
    inet_raw: *mut CassInet,
) -> CassError {
    let inet = ptr_to_ref_mut(inet_raw);

    let input = ptr_to_cstr_n(input_raw, input_length);
    if input.is_none() {
        return CassError::CASS_ERROR_LIB_BAD_PARAMS;
    }

    let input_str = input.unwrap();
    let ip_addr = IpAddr::from_str(input_str);

    match ip_addr {
        Ok(ip_addr) => {
            *inet = ip_addr.into();
            CassError::CASS_OK
        }
        Err(_) => CassError::CASS_ERROR_LIB_BAD_PARAMS,
    }
}

impl From<CassInet> for IpAddr {
    fn from(inet: CassInet) -> Self {
        match inet.address_length {
            CassInetLength::CASS_INET_V4 => {
                let addr_bytes: [cass_uint8_t; CassInetLength::CASS_INET_V4 as usize] = inet
                    .address[0..(CassInetLength::CASS_INET_V4 as usize)]
                    .try_into()
                    .unwrap();
                IpAddr::V4(addr_bytes.into())
            }
            CassInetLength::CASS_INET_V6 => {
                let addr_bytes: [cass_uint8_t; CassInetLength::CASS_INET_V6 as usize] = inet
                    .address[0..(CassInetLength::CASS_INET_V6 as usize)]
                    .try_into()
                    .unwrap();
                IpAddr::V6(addr_bytes.into())
            }
        }
    }
}

impl From<IpAddr> for CassInet {
    fn from(ip_addr: IpAddr) -> Self {
        match ip_addr {
            IpAddr::V4(v4_addr) => {
                let mut address = [0; 16];
                address[0..(CassInetLength::CASS_INET_V4 as usize)]
                    .copy_from_slice(&v4_addr.octets());

                CassInet {
                    address,
                    address_length: CassInetLength::CASS_INET_V4,
                }
            }
            IpAddr::V6(v6_addr) => {
                let mut address = [0; 16];
                address[0..(CassInetLength::CASS_INET_V6 as usize)]
                    .copy_from_slice(&v6_addr.octets());

                CassInet {
                    address,
                    address_length: CassInetLength::CASS_INET_V6,
                }
            }
        }
    }
}
