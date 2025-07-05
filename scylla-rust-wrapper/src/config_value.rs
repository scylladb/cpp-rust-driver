use scylla::statement::{Consistency, SerialConsistency};

use crate::cass_types::CassConsistency;

/// Represents a configuration value that may or may not be set.
/// If a configuration value is unset, it means that the default value
/// should be used.
pub(crate) enum MaybeUnsetConfig<T> {
    Unset,
    Set(T),
}

/// Represents types that can be converted from a C value have the special unset value.
/// This is used to handle cases where a configuration value may not be set,
/// allowing the driver to clearly distinguish between an unset value and a set value.
pub(crate) trait MaybeUnsetConfigValue: Sized {
    type CValue;
    type Error;

    /// Checks if the given C value is considered unset.
    fn is_unset(cvalue: &Self::CValue) -> bool;

    /// Converts a maybe unset C value to a Rust value, returning an error if the value
    /// is invalid.
    fn from_c_value(cvalue: Self::CValue) -> Result<MaybeUnsetConfig<Self>, Self::Error> {
        if Self::is_unset(&cvalue) {
            Ok(MaybeUnsetConfig::Unset)
        } else {
            let rust_value = Self::from_set_c_value(cvalue)?;
            Ok(MaybeUnsetConfig::Set(rust_value))
        }
    }

    /// Converts a **set** C value to a Rust value, returning an error if the value
    /// is invalid or if the value is unset.
    fn from_set_c_value(cvalue: Self::CValue) -> Result<Self, Self::Error>;
}

impl<T: MaybeUnsetConfigValue> MaybeUnsetConfig<T> {
    /// Converts a maybe unset C value to a Rust value, returning an error if the value
    /// is invalid.
    pub(crate) fn from_c_value(cvalue: T::CValue) -> Result<Self, T::Error> {
        <T as MaybeUnsetConfigValue>::from_c_value(cvalue)
    }
}

impl MaybeUnsetConfigValue for Consistency {
    type CValue = CassConsistency;
    type Error = ();

    fn is_unset(cvalue: &Self::CValue) -> bool {
        *cvalue == CassConsistency::CASS_CONSISTENCY_UNKNOWN
    }

    fn from_set_c_value(cvalue: Self::CValue) -> Result<Self, Self::Error> {
        match cvalue {
            CassConsistency::CASS_CONSISTENCY_ANY => Ok(Consistency::Any),
            CassConsistency::CASS_CONSISTENCY_ONE => Ok(Consistency::One),
            CassConsistency::CASS_CONSISTENCY_TWO => Ok(Consistency::Two),
            CassConsistency::CASS_CONSISTENCY_THREE => Ok(Consistency::Three),
            CassConsistency::CASS_CONSISTENCY_QUORUM => Ok(Consistency::Quorum),
            CassConsistency::CASS_CONSISTENCY_ALL => Ok(Consistency::All),
            CassConsistency::CASS_CONSISTENCY_LOCAL_QUORUM => Ok(Consistency::LocalQuorum),
            CassConsistency::CASS_CONSISTENCY_EACH_QUORUM => Ok(Consistency::EachQuorum),
            CassConsistency::CASS_CONSISTENCY_LOCAL_ONE => Ok(Consistency::LocalOne),
            CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL => Ok(Consistency::LocalSerial),
            CassConsistency::CASS_CONSISTENCY_SERIAL => Ok(Consistency::Serial),
            _ => Err(()),
        }
    }
}

impl MaybeUnsetConfigValue for Option<SerialConsistency> {
    type CValue = CassConsistency;
    type Error = ();

    fn is_unset(cvalue: &Self::CValue) -> bool {
        *cvalue == CassConsistency::CASS_CONSISTENCY_UNKNOWN
    }

    fn from_set_c_value(cvalue: Self::CValue) -> Result<Self, Self::Error> {
        match cvalue {
            CassConsistency::CASS_CONSISTENCY_ANY => {
                // This is in line with the CPP Driver: if 0 is passed (which is Consistency::Any),
                // then serial consistency is not set:
                // ```c++
                // if (callback->serial_consistency() != 0) {
                //  flags |= CASS_QUERY_FLAG_SERIAL_CONSISTENCY;
                // }
                // ```
                Ok(None)
            }
            CassConsistency::CASS_CONSISTENCY_LOCAL_SERIAL => {
                Ok(Some(SerialConsistency::LocalSerial))
            }
            CassConsistency::CASS_CONSISTENCY_SERIAL => Ok(Some(SerialConsistency::Serial)),
            _ => Err(()),
        }
    }
}
