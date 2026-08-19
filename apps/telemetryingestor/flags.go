package main

import "strings"

// stringSlice is a flag.Value that accumulates repeated flag occurrences, so
// that "-where a=b -where c=d" collects both values. The standard flag package
// has no built-in repeatable string flag.
type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ",") }

func (s *stringSlice) Set(v string) error {
	*s = append(*s, v)
	return nil
}
