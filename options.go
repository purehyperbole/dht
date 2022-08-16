package dht

import "time"

// FindOption for configuring find requests
type FindOption struct {
	from time.Time
}

// ValuesFrom filters results to only those that were created after a given timestmap
// this is useful for repeat queries where duplicates ideally should be avoided
func ValuesFrom(from time.Time) *FindOption {
	return &FindOption{
		from: from,
	}
}
