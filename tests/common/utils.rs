use bytes::Bytes;
use nebz::NonEmptyBz;

pub fn make_nebz_bytes<BZ>(bz: BZ) -> NonEmptyBz<Bytes>
where
    BZ: AsRef<[u8]>,
{
    NonEmptyBz::new(Bytes::copy_from_slice(bz.as_ref())).expect("bz must be non-empty")
}
