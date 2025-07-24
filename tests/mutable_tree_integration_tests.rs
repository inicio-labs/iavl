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
		value: Bytes,
	},
	Remove(NonEmptyBz<Bytes>),
	Save,
}

enum Terminal {
	Insert {
		key: NonEmptyBz<Bytes>,
		value: Bytes,
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
	value: Option<Bytes>,
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
			value: Bytes::copy_from_slice(value.as_ref()),
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
			value: Bytes::copy_from_slice(value.as_ref()),
			expected,
		}
	}

	fn remove<K>(key: K, expected: RemoveExpected) -> Self
	where
		K: AsRef<[u8]>,
	{
		Self::Remove { key: utils::make_nebz_bytes(key), expected }
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
			value: value.into().as_ref().map(AsRef::as_ref).map(Bytes::copy_from_slice),
			version: U63::new(version).unwrap(),
			size: U63::new(size).unwrap(),
		}
	}
}

impl RemoveExpected {
	fn new(removed: bool, version: u64, size: u64) -> Self {
		Self { removed, version: U63::new(version).unwrap(), size: U63::new(size).unwrap() }
	}
}

impl SaveExpected {
	fn new<H>(version: u64, size: u64, hex_hash: H) -> Self
	where
		H: AsRef<[u8]>,
	{
		let hash = const_hex::decode(hex_hash).unwrap().try_into().unwrap();

		Self { version: U63::new(version).unwrap(), size: U63::new(size).unwrap(), hash }
	}
}

#[rstest]
#[case::new_key_insertion(
    vec![],
    Terminal::insert("perfect", "blue", InsertExpected::new(false, 0, "blue", 0, 1)),
)]
#[case::key_insert_with_empty_value(
	vec![],
	Terminal::insert("only", "", InsertExpected::new(false, 0, "", 0, 1))
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
#[case::key_insert_with_smaller_idx_after_save(
    vec![Op::insert("remote", "control"), Op::Save],
    Terminal::insert("radio", "access", InsertExpected::new(false, 0, "access", 1, 2)),
)]
#[case::remove_nonexistent_key(
    vec![Op::insert("one", "plus"), Op::Save],
    Terminal::remove("nothing", RemoveExpected::new(false, 1, 1)),
)]
#[case::remove_existing_key(
    vec![Op::insert("one", "plus"), Op::Save],
    Terminal::remove("one", RemoveExpected::new(true, 1, 0)),
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
#[case::rr_heavy_leading_to_rotation_while_insertion_followed_by_save(
    vec![
        Op::insert("a", "a"),
        Op::insert("b", "b"),
        Op::insert("c", "c"),
        Op::insert("d", "d"),
    ],
    Terminal::save(
        SaveExpected::new(1, 4, "B06D89CB2E3B3697B3E0D19234200049DE83914842B179C5E4703D36C85D0DD5"),
    ),
)]
#[case::ll_heavy_leading_to_rotation_while_insertion_followed_by_save(
    vec![
        Op::insert("d", "d"),
        Op::insert("c", "c"),
        Op::insert("b", "b"),
        Op::insert("a", "a"),
    ],
    Terminal::save(
        SaveExpected::new(1, 4, "B06D89CB2E3B3697B3E0D19234200049DE83914842B179C5E4703D36C85D0DD5"),
    ),
)]
#[case::rl_heavy_leading_to_rotation_while_insertion_followed_by_save(
    vec![
        Op::insert("aaaa", "aaaa"),
        Op::insert("bbbb", "bbbb"),
        Op::insert("bbba", "bbba"),
        Op::insert("bbaa", "bbaa"),
    ],
    Terminal::save(
        SaveExpected::new(1, 4, "BE55AB794D27BAECE92A3F5F2EA107913119F953B49680F52E3AF2F045CD90BC"),
    ),
)]
#[case::lr_heavy_leading_to_rotation_while_insertion_followed_by_save(
    vec![
        Op::insert("bbbb", "bbbb"),
        Op::insert("aaaa", "aaaa"),
        Op::insert("aaab", "aaab"),
        Op::insert("aabb", "aabb"),
    ],
    Terminal::save(
        SaveExpected::new(1, 4, "7372CA8D9C09214A26ADBF85D7DB2D8A563C2E7382C76277E30FBF7F091FA04E"),
    )
)]
#[case::lr_heavy_leading_to_rotation_while_removal_followed_by_save(
    vec![
        Op::insert("a", "a"),
        Op::insert("b", "b"),
        Op::insert("c", "c"),
        Op::insert("ab", "ab"),
        Op::insert("ac", "ac"),
        Op::remove("b"),
    ],
    Terminal::save(
        SaveExpected::new(1, 4, "1543F99F8C08C5847B0F4E4CCB87196855F1DC6A7BBF13713307154C55192580"),
    ),
)]
#[case::ll_heavy_leading_to_rotation_while_removal_followed_by_save(
    vec![
        Op::insert("bbbb", "bbbb"),
        Op::insert("cccc", "cccc"),
        Op::insert("ccca", "ccca"),
        Op::insert("bbba", "bbba"),
        Op::insert("bbaa", "bbaa"),
        Op::remove("ccca"),
    ],
    Terminal::save(
        SaveExpected::new(1, 4, "A51B898158C3BC1DAC9AF9E0D05AB18D8386F51374D17B661ADA941AD51624A2"),
    ),
)]
#[case::rl_heavy_leading_to_rotation_while_removal_followed_by_save(
    vec![
       Op::insert("dddd", "dddd"),
       Op::insert("a", "a"),
       Op::insert("b", "b"),
       Op::insert("dddc", "dddc"),
       Op::insert("ddcc", "ddcc"),
       Op::remove("a"),
    ],
    Terminal::save(
        SaveExpected::new(1, 4, "984464A9B82CF68DD19D18FE1F47007C8E51AE16C4942381C81EC303B07F450C"),
    ),
)]
#[case::rr_heavy_leading_to_rotation_while_removal_followed_by_save(
    vec![
        Op::insert("c", "c"),
        Op::insert("b", "b"),
        Op::insert("a", "a"),
        Op::insert("d", "d"),
        Op::insert("e", "e"),
        Op::remove("a"),
    ],
    Terminal::save(
        SaveExpected::new(1, 4, "B950F5CA79A74DB926E6EFF0BC057727A4CC9BB766A04E91079D1C10881E190B"),
    ),
)]
fn terminal_tree_operation_post_setup_works(#[case] setup: Vec<Op>, #[case] terminal: Terminal) {
	// Arrange
	let mut tree = TestContext::new().tree;

	setup.into_iter().for_each(|op| exec_operation(&mut tree, op));

	match terminal {
		Terminal::Insert { key, value, expected } => {
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
		},
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
		},
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
		},
	}
}

fn exec_operation<DB>(tree: &mut MutableTree<DB>, op: Op)
where
	DB: MutKVStore + KVStore + Clone,
{
	match op {
		Op::Insert { key, value } => {
			tree.insert(key, value).unwrap();
		},
		Op::Remove(key) => {
			tree.remove(key).unwrap();
		},
		Op::Save => {
			tree.save().unwrap();
		},
	}
}
