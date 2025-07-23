use core::{cmp, mem};

use oblux::{U7, U63};

use crate::{
	kvstore::KVStore,
	node::{
		ArlockNode, DraftedNode,
		info::Drafted,
		ndb::{FetchedNode, NodeDb},
	},
};

use super::{Child, InnerNode, InnerNodeError, Result};

impl InnerNode<Drafted> {
	// TODO: make it simpler and concise; devise strategy to reduce key clones
	/// Returns
	pub fn make_balanced<DB>(&mut self, ndb: &NodeDb<DB>) -> Result<Option<Self>>
	where
		DB: KVStore,
	{
		let extract_full = |child: &mut Child| -> Result<_> {
			let node = match child.extract()? {
				Child::Full(full) => full,
				Child::Part(nk) => ndb
					.fetch_one_node(&nk)?
					.map(|node| match node {
						FetchedNode::Deserialized(denode) => denode.into_saved_checked(&nk),
						FetchedNode::EmptyRoot | FetchedNode::ReferenceRoot(_) => {
							Err(InnerNodeError::InvalidChild)
						},
					})
					.transpose()?
					.map(From::from)
					.ok_or(InnerNodeError::ChildNotFound)?,
			};

			Ok(node)
		};

		let height_size_pair = |node: &ArlockNode| -> Result<_> {
			node.read().map(|gnode| (gnode.height(), gnode.size())).map_err(From::from)
		};

		let left = extract_full(self.left_mut())?;
		let right = extract_full(self.right_mut())?;

		let (left_height, left_size) = height_size_pair(&left)?;
		let (right_height, right_size) = height_size_pair(&right)?;

		let diff = left_height.to_signed() - right_height.to_signed();

		if (-1..=1).contains(&diff) {
			*self.left_mut() = Child::Full(left);
			*self.right_mut() = Child::Full(right);

			return Ok(None);
		}

		if diff > 1 {
			let mut gleft_mut = left.write()?;

			// unwrap is safe because left must be inner when diff > 1
			let ll = gleft_mut.left_mut().map(extract_full).transpose()?.unwrap();
			let lr = gleft_mut.right_mut().map(extract_full).transpose()?.unwrap();

			// TODO: `gleft_mut` is unnecessarily mut beyond this.
			// Downgrade to read guard when feature `rwlock_downgrade` lands in stable.
			// https://github.com/rust-lang/rust/pull/143191

			let (ll_height, ll_size) = height_size_pair(&ll)?;
			let (lr_height, lr_size) = height_size_pair(&lr)?;

			let left_diff = ll_height.to_signed() - lr_height.to_signed();

			if left_diff >= 0 {
				// left-left case: one right rotation on self.

				let new_right = {
					// TODO: ascertain whether 1 can be directly added without overflow checks
					let new_right_height = cmp::max(right_height, lr_height)
						.get()
						.checked_add(1)
						.and_then(U7::new)
						.ok_or(InnerNodeError::Overflow)?;

					// TODO: ascertain whether additions can be direct without overflow checks
					let new_right_size = right_size
						.get()
						.checked_add(lr_size.get())
						.and_then(U63::new)
						.ok_or(InnerNodeError::Overflow)?;

					InnerNode::builder()
						.key(self.key().clone())
						.height(new_right_height)
						.size(new_right_size)
						.left(Child::Full(lr))
						.right(Child::Full(right))
						.build()
				};

				let new_root = {
					// TODO: ascertain whether 1 can be directly added without overflow checks
					let new_root_height = cmp::max(ll_height, new_right.height())
						.get()
						.checked_add(1)
						.and_then(U7::new)
						.ok_or(InnerNodeError::Overflow)?;

					// TODO: ascertain whether additions can be direct without overflow checks
					let new_root_size = ll_size
						.get()
						.checked_add(new_right.size().get())
						.and_then(U63::new)
						.ok_or(InnerNodeError::Overflow)?;

					InnerNode::builder()
						.key(gleft_mut.key().cloned())
						.height(new_root_height)
						.size(new_root_size)
						.left(Child::Full(ll))
						.right(Child::Full(DraftedNode::from(new_right).into()))
						.build()
				};

				return Ok(Some(mem::replace(self, new_root)));
			}

			// left-right case: one left rotation on left, and then one right rotation on self

			let mut glr_mut = lr.write()?;

			let lrl = glr_mut.left_mut().map(extract_full).transpose()?.unwrap();
			let lrr = glr_mut.right_mut().map(extract_full).transpose()?.unwrap();

			// TODO: `glr_mut` is unnecessarily mut beyond this.
			// Downgrade to read guard when feature `rwlock_downgrade` lands in stable.
			// https://github.com/rust-lang/rust/pull/143191

			let (lrl_height, lrl_size) = height_size_pair(&lrl)?;
			let (lrr_height, lrr_size) = height_size_pair(&lrr)?;

			let new_left = {
				// TODO: ascertain whether 1 can be directly added without overflow checks
				let new_left_height = cmp::max(ll_height, lrl_height)
					.get()
					.checked_add(1)
					.and_then(U7::new)
					.ok_or(InnerNodeError::Overflow)?;

				// TODO: ascertain whether additions can be direct without overflow checks
				let new_left_size = ll_size
					.get()
					.checked_add(lrl_size.get())
					.and_then(U63::new)
					.ok_or(InnerNodeError::Overflow)?;

				InnerNode::builder()
					.key(gleft_mut.key().cloned())
					.height(new_left_height)
					.size(new_left_size)
					.left(Child::Full(ll))
					.right(Child::Full(lrl))
					.build()
			};

			let new_right = {
				// TODO: ascertain whether 1 can be directly added without overflow checks
				let new_right_height = cmp::max(lrr_height, right_height)
					.get()
					.checked_add(1)
					.and_then(U7::new)
					.ok_or(InnerNodeError::Overflow)?;

				// TODO: ascertain whether additions can be direct without overflow checks
				let new_right_size = lrr_size
					.get()
					.checked_add(right_size.get())
					.and_then(U63::new)
					.ok_or(InnerNodeError::Overflow)?;

				InnerNode::builder()
					.key(self.key().clone())
					.height(new_right_height)
					.size(new_right_size)
					.left(Child::Full(lrr))
					.right(Child::Full(right))
					.build()
			};

			let new_root = {
				// TODO: ascertain whether 1 can be directly added without overflow checks
				let new_root_height = cmp::max(new_left.height(), new_right.height())
					.get()
					.checked_add(1)
					.and_then(U7::new)
					.ok_or(InnerNodeError::Overflow)?;

				// TODO: ascertain whether additions can be direct without overflow checks
				let new_root_size = new_left
					.size()
					.get()
					.checked_add(new_right.size().get())
					.and_then(U63::new)
					.ok_or(InnerNodeError::Overflow)?;

				InnerNode::builder()
					.key(glr_mut.key().cloned())
					.height(new_root_height)
					.size(new_root_size)
					.left(Child::Full(DraftedNode::from(new_left).into()))
					.right(Child::Full(DraftedNode::from(new_right).into()))
					.build()
			};

			return Ok(Some(mem::replace(self, new_root)));
		}

		let mut gright_mut = right.write()?;

		// unwrap is safe because left must be inner when diff < -1
		let rl = gright_mut.left_mut().map(extract_full).transpose()?.unwrap();
		let rr = gright_mut.right_mut().map(extract_full).transpose()?.unwrap();

		// TODO: `gright_mut` is unnecessarily mut beyond this.
		// Downgrade to read guard when feature `rwlock_downgrade` lands in stable.
		// https://github.com/rust-lang/rust/pull/143191

		let (rl_height, rl_size) = height_size_pair(&rl)?;
		let (rr_height, rr_size) = height_size_pair(&rr)?;

		let right_diff = rl_height.to_signed() - rr_height.to_signed();

		if right_diff <= 0 {
			// right-right case: one left rotation on self.
			let new_left = {
				// TODO: ascertain whether 1 can be directly added without overflow checks
				let new_left_height = cmp::max(left_height, rl_height)
					.get()
					.checked_add(1)
					.and_then(U7::new)
					.ok_or(InnerNodeError::Overflow)?;

				// TODO: ascertain whether additions can be direct without overflow checks
				let new_left_size = left_size
					.get()
					.checked_add(rl_size.get())
					.and_then(U63::new)
					.ok_or(InnerNodeError::Overflow)?;

				InnerNode::builder()
					.key(self.key().clone())
					.height(new_left_height)
					.size(new_left_size)
					.left(Child::Full(left))
					.right(Child::Full(rl))
					.build()
			};

			let new_root = {
				// TODO: ascertain whether 1 can be directly added without overflow checks
				let new_root_height = cmp::max(new_left.height(), rr_height)
					.get()
					.checked_add(1)
					.and_then(U7::new)
					.ok_or(InnerNodeError::Overflow)?;

				// TODO: ascertain whether additions can be direct without overflow checks
				let new_root_size = rr_size
					.get()
					.checked_add(new_left.size().get())
					.and_then(U63::new)
					.ok_or(InnerNodeError::Overflow)?;

				InnerNode::builder()
					.key(gright_mut.key().cloned())
					.height(new_root_height)
					.size(new_root_size)
					.left(Child::Full(DraftedNode::from(new_left).into()))
					.right(Child::Full(rr))
					.build()
			};

			return Ok(Some(mem::replace(self, new_root)));
		}

		// right-left case: one right rotation on right, and then one left rotation on self

		let mut grl_mut = rl.write()?;

		let rll = grl_mut.left_mut().map(extract_full).transpose()?.unwrap();
		let rlr = grl_mut.right_mut().map(extract_full).transpose()?.unwrap();

		// TODO: `grl_mut` is unnecessarily mut beyond this.
		// Downgrade to read guard when feature `rwlock_downgrade` lands in stable.
		// https://github.com/rust-lang/rust/pull/143191

		let (rll_height, rll_size) = height_size_pair(&rll)?;
		let (rlr_height, rlr_size) = height_size_pair(&rlr)?;

		let new_left = {
			// TODO: ascertain whether 1 can be directly added without overflow checks
			let new_left_height = cmp::max(left_height, rll_height)
				.get()
				.checked_add(1)
				.and_then(U7::new)
				.ok_or(InnerNodeError::Overflow)?;

			// TODO: ascertain whether additions can be direct without overflow checks
			let new_left_size = left_size
				.get()
				.checked_add(rll_size.get())
				.and_then(U63::new)
				.ok_or(InnerNodeError::Overflow)?;

			InnerNode::builder()
				.key(self.key().clone())
				.height(new_left_height)
				.size(new_left_size)
				.left(Child::Full(left))
				.right(Child::Full(rll))
				.build()
		};

		let new_right = {
			// TODO: ascertain whether 1 can be directly added without overflow checks
			let new_right_height = cmp::max(rlr_height, rr_height)
				.get()
				.checked_add(1)
				.and_then(U7::new)
				.ok_or(InnerNodeError::Overflow)?;

			// TODO: ascertain whether additions can be direct without overflow checks
			let new_right_size = rlr_size
				.get()
				.checked_add(rr_size.get())
				.and_then(U63::new)
				.ok_or(InnerNodeError::Overflow)?;

			InnerNode::builder()
				.key(gright_mut.key().cloned())
				.height(new_right_height)
				.size(new_right_size)
				.left(Child::Full(rlr))
				.right(Child::Full(rr))
				.build()
		};

		let new_root = {
			// TODO: ascertain whether 1 can be directly added without overflow checks
			let new_root_height = cmp::max(new_left.height(), new_right.height())
				.get()
				.checked_add(1)
				.and_then(U7::new)
				.ok_or(InnerNodeError::Overflow)?;

			// TODO: ascertain whether additions can be direct without overflow checks
			let new_root_size = new_left
				.size()
				.get()
				.checked_add(new_right.size().get())
				.and_then(U63::new)
				.ok_or(InnerNodeError::Overflow)?;

			InnerNode::builder()
				.key(grl_mut.key().cloned())
				.height(new_root_height)
				.size(new_root_size)
				.left(Child::Full(DraftedNode::from(new_left).into()))
				.right(Child::Full(DraftedNode::from(new_right).into()))
				.build()
		};

		Ok(Some(mem::replace(self, new_root)))
	}
}
