package cedar

// Status reports the following statistics of the cedar:
//	keys:		number of keys that are in the cedar,
//	nodes:		number of trie nodes (slots in the base array) has been taken,
//	size:			the size of the base array used by the cedar,
//	capacity:		the capicity of the base array used by the cedar.
func (da *Cedar) Status() (keys, nodes, size, capacity int) {
	for i := 0; i < da.size; i++ {
		n := da.array[i]
		if n.Check >= 0 {
			nodes++
			if n.Value >= 0 {
				keys++
			}
		}
	}
	return keys, nodes, da.size, da.capacity
}

// Jump travels from a node `from` to another node `to` by following the path `path`.
// For example, if the following keys were inserted:
//	id	key
//	19	abc
//	23	ab
//	37	abcd
// then
//	Jump([]byte("ab"), 0) = 23, nil		// reach "ab" from root
//	Jump([]byte("c"), 23) = 19, nil			// reach "abc" from "ab"
//	Jump([]byte("cd"), 23) = 37, nil		// reach "abcd" from "ab"
func (da *Cedar) Jump(path []byte, from int) (to int, err error) {
	for _, b := range path {
		if da.array[from].Value >= 0 {
			return from, ErrNoPath
		}
		to = da.array[from].base() ^ int(b)
		if da.array[to].Check != from {
			return from, ErrNoPath
		}
		from = to
	}
	return to, nil
}

// Key returns the key of the node with the given `id`.
// It will return ErrNoPath, if the node does not exist.
func (da *Cedar) Key(id int) (key []byte, err error) {
	for id > 0 {
		from := da.array[id].Check
		if from < 0 {
			return nil, ErrNoPath
		}
		if char := byte(da.array[from].base() ^ id); char != 0 {
			key = append(key, char)
		}
		id = from
	}
	if id != 0 || len(key) == 0 {
		return nil, ErrInvalidKey
	}
	for i := 0; i < len(key)/2; i++ {
		key[i], key[len(key)-i-1] = key[len(key)-i-1], key[i]
	}
	return key, nil
}

// Value returns the value of the node with the given `id`.
// It will return ErrNoValue, if the node does not have a value.
func (da *Cedar) vKeyOf(id int) (value int, err error) {
	value = da.array[id].Value
	if value >= 0 {
		return value, nil
	}
	to := da.array[id].base()
	if da.array[to].Check == id && da.array[to].Value >= 0 {
		return da.array[to].Value, nil
	}
	return 0, ErrNoValue
}

// Insert adds a key-value pair into the cedar.
// It will return ErrInvalidValue, if value < 0 or >= valueLimit.
func (da *Cedar) Insert(key []byte, value interface{}) error {
	k := da.vKey()
	klen := len(key)
	p := da.get(key, 0, 0)
	//fmt.Printf("k:%s, v:%d\n", string(key), value)
	da.array[p].Value = k
	da.info[p].End = true
	da.vals[k] = nvalue{Len: klen, Value: value}
	return nil
}

// Update increases the value associated with the `key`.
// The `key` will be inserted if it is not in the cedar.
// It will return ErrInvalidValue, if the updated value < 0 or >= valueLimit.
func (da *Cedar) Update(key []byte, value int) error {
	id := da.get(key, 0, 0)
	p := &da.array[id].Value
	if *p+value < 0 || *p+value >= valueLimit {
		return ErrInvalidValue
	}
	*p += value
	return nil
}

// Delete removes a key-value pair from the cedar.
// It will return ErrNoPath, if the key has not been added.
func (da *Cedar) Delete(key []byte) error {
	// if the path does not exist, or the end is not a leaf, nothing to delete
	to, err := da.Jump(key, 0)
	if err != nil {
		return ErrNoPath
	}

	if da.array[to].Value < 0 {
		base := da.array[to].base()
		if da.array[base].Check == to {
			to = base
		}
	}

	for {
		from := da.array[to].Check
		base := da.array[from].base()
		label := byte(to ^ base)

		// if `to` has sibling, remove `to` from the sibling list, then stop
		if da.info[to].Sibling != 0 || da.info[from].Child != label {
			// delete the label from the child ring first
			da.popSibling(from, base, label)
			// then release the current node `to` to the empty node ring
			da.pushEnode(to)
			break
		}
		// otherwise, just release the current node `to` to the empty node ring
		da.pushEnode(to)
		// then check its parent node
		to = from
	}
	return nil
}

// Get returns the value associated with the given `key`.
// It is equivalent to
//		id, err1 = Jump(key)
//		value, err2 = Value(id)
// Thus, it may return ErrNoPath or ErrNoValue,
func (da *Cedar) Get(key []byte) (value interface{}, err error) {
	to, err := da.Jump(key, 0)
	if err != nil {
		return 0, err
	}
	vk, err := da.vKeyOf(to)
	if err != nil {
		return nil, ErrNoValue
	}
	if v, ok := da.vals[vk]; ok {
		return v.Value, nil
	}
	return nil, ErrNoValue
}

func (da *Cedar) GetByNid(nid int) (value interface{}, err error) {
	vk, err := da.vKeyOf(nid)
	if err != nil {
		return nil, ErrNoValue
	}
	if v, ok := da.vals[vk]; ok {
		return v.Value, nil
	}
	return nil, ErrNoValue
}

func (da *Cedar) GetLabel(nid int) byte {
	from := da.array[nid].Check
	base := da.array[from].base()

	return byte(nid ^ base)
}

// Match multiple subsequence in seq and return tokens
// Wildcard in the end (or start) of the seq means zero or any amount of symbols
// Wildcard in the middle of the seq means one symbol or more
func (da *Cedar) MatchWildcard(seq []byte, nid int, cb func(nid int, key []byte, value interface{})) {
	wildcard := -1
	e := len(seq) - 1

	if nid > 0 && da.GetLabel(nid) == '*' {
		wildcard = nid
	}

	var eof bool
	for i, b := range seq {
		eof = i == e
		// does the current node has a b as child?
		if da.hasLabel(nid, b) {
			if b != '*' {
				// if yes and b != '*' try to find the wildcard for the same node
				//and launch the MatchWildcard for the new branch
				if wildcard, _ := da.child(nid, '*'); wildcard > -1 {
					if i == 0 {
						da.MatchWildcard(seq, wildcard, cb)
					} else {
						da.MatchWildcard(seq[i+1:], wildcard, cb)
					}

				}
			}

			nid, _ = da.child(nid, b)

			// if it's EOF and we haven't reached the leaf with data
			// check the next think: does the current node has a child *?
			// Maybe this child is leaf with data?
			if eof && !da.hasData(nid) {
				if wildcard, _ = da.child(nid, '*'); wildcard != -1 {
					nid = wildcard
				} else {
					break
				}
			}

			if wildcard != -1 {
				if !eof {
					// if we found a new node which is not *
					// but we have found the wildcard in the previous step
					// launch the MatchWildcard for the old rule with *
					// example: we have 2 rules: c*s and c*rs, seq = cars
					// both rules are matched but rule c*rs is stronger than c*s
					// we must keep the matching process for the c*s
					da.MatchWildcard(seq[i+1:], wildcard, cb)
				}
				wildcard = -1
			}

		} else {

			// if node doesn't contain b
			if wildcard > -1 {
				// but in the previous step we found the wildcard
				// keep the wildcard' nid (node id)
				nid = wildcard
				if i != e {
					continue
				}
				// if node doesn't contain b and we don't have the wildcard
				// let's find it
			} else if wildcard, _ = da.child(nid, '*'); wildcard == -1 {
				break
			} else {
				// if we found a wildcard, leave the wildcard as the new nid
				nid = wildcard
			}
		}

		// if it's end of seq or nid contains wildcard
		// check does this node contain any data or not
		if eof || nid == wildcard {

			if v, err := da.GetByNid(nid); nil == err {
				k, _ := da.Key(nid)
				cb(nid, k, v)
			}

			if !eof {
				// if current node doesn't have a child, break the loop
				// for example, rule: "cars*" and seq: "cars for sale"
				if !da.hasChild(nid) {
					break
				}
			} else if nid != wildcard {
				// for example we have 2 rules: "cars*", "cars" and seq: "cars"
				// current node is 's' and this is EOF
				// we should follow both rules
				if wildcard, _ = da.child(nid, '*'); wildcard > -1 {
					if v, err := da.GetByNid(wildcard); nil == err {
						k, _ := da.Key(wildcard)
						cb(wildcard, k, v)
					}
				}
			}

		}
		continue
	}
}

// PrefixMatch returns a list of at most `num` nodes which match the prefix of the key.
// If `num` is 0, it returns all matches.
// For example, if the following keys were inserted:
//	id	key
//	19	abc
//	23	ab
//	37	abcd
// then
//	PrefixMatch([]byte("abc"), 1) = [ 23 ]				// match ["ab"]
//	PrefixMatch([]byte("abcd"), 0) = [ 23, 19, 37]		// match ["ab", "abc", "abcd"]
func (da *Cedar) PrefixMatch(key []byte, num int) (ids []int) {
	for from, i := 0, 0; i < len(key); i++ {
		to, err := da.Jump(key[i:i+1], from)
		if err != nil {
			break
		}
		if _, err := da.vKeyOf(to); err == nil {
			ids = append(ids, to)
			num--
			if num == 0 {
				return
			}
		}
		from = to
	}
	return
}

// PrefixPredict returns a list of at most `num` nodes which has the key as their prefix.
// These nodes are ordered by their keys.
// If `num` is 0, it returns all matches.
// For example, if the following keys were inserted:
//	id	key
//	19	abc
//	23	ab
//	37	abcd
// then
//	PrefixPredict([]byte("ab"), 2) = [ 23, 19 ]			// predict ["ab", "abc"]
//	PrefixPredict([]byte("ab"), 0) = [ 23, 19, 37 ]		// predict ["ab", "abc", "abcd"]
func (da *Cedar) PrefixPredict(key []byte, num int) (ids []int) {
	root, err := da.Jump(key, 0)
	if err != nil {
		return
	}
	for from, err := da.begin(root); err == nil; from, err = da.next(from, root) {
		ids = append(ids, from)
		num--
		if num == 0 {
			return
		}
	}
	return
}

func (da *Cedar) begin(from int) (to int, err error) {
	for c := da.info[from].Child; c != 0; {
		to = da.array[from].base() ^ int(c)
		c = da.info[to].Child
		from = to
	}
	if da.array[from].base() > 0 {
		return da.array[from].base(), nil
	}
	return from, nil
}

func (da *Cedar) next(from int, root int) (to int, err error) {
	c := da.info[from].Sibling
	for c == 0 && from != root && da.array[from].Check >= 0 {
		from = da.array[from].Check
		c = da.info[from].Sibling
	}
	if from == root {
		return 0, ErrNoPath
	}
	from = da.array[da.array[from].Check].base() ^ int(c)
	return da.begin(from)
}
