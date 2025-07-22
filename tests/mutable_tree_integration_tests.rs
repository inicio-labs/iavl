mod common;

use bytes::Bytes;
use common::TestContext;
use iavl::{
    Get, MutableTree,
    kvstore::{KVStore, MutKVStore},
};
use nebz::NonEmptyBz;
use oblux::U63;
use rstest::rstest;

use self::common::utils;

enum Op {
    Insert {
        key: NonEmptyBz<Bytes>,
        value: NonEmptyBz<Bytes>,
    },
    Remove(NonEmptyBz<Bytes>),
    Save,
}

enum Terminal {
    Insert {
        key: NonEmptyBz<Bytes>,
        value: NonEmptyBz<Bytes>,
        expected: InsertExpected,
    },
    Remove {
        key: NonEmptyBz<Bytes>,
        expected: RemoveExpected,
    },
    Save {
        expected: SaveExpected,
    },
}

struct InsertExpected {
    updated: bool,
    idx: U63,
    value: Option<NonEmptyBz<Bytes>>,
    version: U63,
    size: U63,
}

struct RemoveExpected {
    removed: bool,
    version: U63,
    size: U63,
}

struct SaveExpected {
    version: U63,
    size: U63,
    hash: [u8; 32],
}

impl Op {
    fn insert<K, V>(key: K, value: V) -> Self
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        Self::Insert {
            key: utils::make_nebz_bytes(key),
            value: utils::make_nebz_bytes(value),
        }
    }

    fn remove<K>(key: K) -> Self
    where
        K: AsRef<[u8]>,
    {
        Self::Remove(utils::make_nebz_bytes(key))
    }
}

impl Terminal {
    fn insert<K, V>(key: K, value: V, expected: InsertExpected) -> Self
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        Self::Insert {
            key: utils::make_nebz_bytes(key),
            value: utils::make_nebz_bytes(value),
            expected,
        }
    }

    fn remove<K>(key: K, expected: RemoveExpected) -> Self
    where
        K: AsRef<[u8]>,
    {
        Self::Remove {
            key: utils::make_nebz_bytes(key),
            expected,
        }
    }

    fn save(expected: SaveExpected) -> Self {
        Self::Save { expected }
    }
}

impl InsertExpected {
    fn new<V>(updated: bool, idx: u64, value: impl Into<Option<V>>, version: u64, size: u64) -> Self
    where
        V: AsRef<[u8]>,
    {
        Self {
            updated,
            idx: U63::new(idx).unwrap(),
            value: value.into().map(utils::make_nebz_bytes),
            version: U63::new(version).unwrap(),
            size: U63::new(size).unwrap(),
        }
    }
}

impl RemoveExpected {
    fn new(removed: bool, version: u64, size: u64) -> Self {
        Self {
            removed,
            version: U63::new(version).unwrap(),
            size: U63::new(size).unwrap(),
        }
    }
}

impl SaveExpected {
    fn new<H>(version: u64, size: u64, hex_hash: H) -> Self
    where
        H: AsRef<[u8]>,
    {
        let hash = const_hex::decode(hex_hash).unwrap().try_into().unwrap();

        Self {
            version: U63::new(version).unwrap(),
            size: U63::new(size).unwrap(),
            hash,
        }
    }
}

#[rstest]
#[case::new_key_insertion(
    vec![],
    Terminal::insert("perfect", "blue", InsertExpected::new(false, 0, "blue", 0, 1)),
)]
#[case::key_update_same_version(
    vec![Op::insert("log", "in")],
    Terminal::insert("log", "off", InsertExpected::new(true, 0, "off", 0, 1)),
)]
#[case::key_reinsert_after_removal(
    vec![Op::insert("white", "paper"), Op::remove("white")],
    Terminal::insert("white", "line", InsertExpected::new(false, 0, "line", 0, 1)),
)]
#[case::key_insert_after_save(
    vec![Op::insert("radio", "control"), Op::Save],
    Terminal::insert("remote", "access", InsertExpected::new(false, 1, "access", 1, 2)),
)]
#[case::remove_nonexistent_key(
    vec![Op::insert("one", "plus"), Op::Save],
    Terminal::remove("nothing", RemoveExpected::new(false, 1, 1)),
)]
#[case::save_empty_tree(
    vec![],
    Terminal::save(
        SaveExpected::new(1, 0, "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"),
    ),
)]
#[case::save_tree_with_one_unsaved_key(
    vec![Op::insert("first", "principle")],
    Terminal::save(
        SaveExpected::new(1, 1, "54B3DF08491C27F329505402696AF6702076154F52CC9EE7FE2A90CCB087A54C"),
    ),
)]
#[case::save_tree_with_two_unsaved_keys(
    vec![Op::insert("single", "moon"), Op::insert("multiple", "stars")],
    Terminal::save(
        SaveExpected::new(1, 2, "24182B8FAA85723C2412F8048FB11969C8E793E84417EAD08919279469D59C1C"),
    ),
)]
#[case::save_tree_with_new_root_having_been_previously_saved(
    vec![
        Op::insert("london", "wheel"),
        Op::insert("dublin", "spire"),
        Op::insert("chicago", "bean"),
        Op::Save,
        Op::remove("london"),
    ],
    Terminal::save(
        SaveExpected::new(2, 2, "8CAD566B3364205E190849436169B33221AEA4D8756B26AA95501A428B7D3F96"),
    ),
)]
fn insert_and_get_works(#[case] setup: Vec<Op>, #[case] terminal: Terminal) {
    // Arrange
    let mut tree = TestContext::new().tree;

    setup
        .into_iter()
        .for_each(|op| exec_operation(&mut tree, op));

    match terminal {
        Terminal::Insert {
            key,
            value,
            expected,
        } => {
            // Act
            let updated = tree.insert(key.clone(), value).unwrap();

            // Assert
            let (idx, ret_value) = tree.get(key).unwrap();
            let version = tree.version();
            let size = tree.size();

            assert_eq!(updated, expected.updated);
            assert_eq!(idx, expected.idx);
            assert_eq!(ret_value, expected.value);
            assert_eq!(version, expected.version);
            assert_eq!(size, expected.size);
        }
        Terminal::Remove { key, expected } => {
            // Act
            let removed = tree.remove(key.as_ref()).unwrap();

            // Assert
            assert!(matches!(tree.get(key).unwrap(), (_, None)));

            let version = tree.version();
            let size = tree.size();

            assert_eq!(removed, expected.removed);
            assert_eq!(version, expected.version);
            assert_eq!(size, expected.size);
        }
        Terminal::Save { expected } => {
            // Act
            let version = tree.save().unwrap();

            // Assert
            let size = tree.size();
            let hash = tree.saved_hash();

            assert_eq!(version, expected.version);
            assert_eq!(tree.version(), expected.version);
            assert_eq!(size, expected.size);
            assert_eq!(hash, expected.hash);
        }
    }
}

fn exec_operation<DB>(tree: &mut MutableTree<DB>, op: Op)
where
    DB: MutKVStore + KVStore + Clone,
{
    match op {
        Op::Insert { key, value } => {
            tree.insert(key, value).unwrap();
        }
        Op::Remove(key) => {
            tree.remove(key).unwrap();
        }
        Op::Save => {
            tree.save().unwrap();
        }
    }
}
