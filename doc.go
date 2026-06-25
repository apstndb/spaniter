// Package spaniter adapts Cloud Spanner row streams to Go standard iterators.
//
// The package is intentionally lower-level than formatters and writers: it owns
// only iterator lifecycle concerns such as RowIterator.Stop, result metadata,
// and post-drain query stats. Formatting, headers, and export policy stay in
// callers.
package spaniter
