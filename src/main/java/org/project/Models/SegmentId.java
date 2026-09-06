package org.project.Models;

/**
 * Identity of a segment, independent of where/whether its file is currently open.
 * FileStore maps this to a physical path; Segment itself never touches a File/Path.
 */
public record SegmentId(long value) {
}
