package com.hansight.hadoopsearcher.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by zhachao on 15-9-15.
 */
public final class PathFilters {
    private PathFilters() { }

    /**
     * This filter accepts all paths that are not 'hidden'
     * where 'hidden' is defined as paths whose name begins with
     * either a '.' or a '_'
     */
    public static final org.apache.hadoop.fs.PathFilter EXCLUDE_HIDDEN_PATHS_FILTER = new org.apache.hadoop.fs.PathFilter() {
        @Override
        public boolean accept(Path p) {
            String name = p.getName();
            return !(name.startsWith(".") || name.startsWith("_"));
        }
    };

    /**
     * This filter accepts all paths
     */
    public static final org.apache.hadoop.fs.PathFilter ACCEPT_ALL_PATHS_FILTER = new org.apache.hadoop.fs.PathFilter() {
        @Override
        public boolean accept(Path path) {
            return true;
        }
    };

    /**
     * Creates a filter that accepts all paths that are files, and excludes
     * all paths that are directories
     *
     * @param conf job conf
     * @return the path filter
     */
    public static org.apache.hadoop.fs.PathFilter newExcludeDirectoriesFilter(Configuration conf) {
        return new ExcludeDirectoriesOrFilesFilter(conf,
                ExcludeDirectoriesOrFilesFilter.Mode.EXCLUDE_DIRECTORIES);
    }

    /**
     * Creates a filter that accepts all paths that are directories, and excludes
     * all paths that are files
     *
     * @param conf job conf
     * @return the path filter
     */
    public static org.apache.hadoop.fs.PathFilter newExcludeFilesFilter(Configuration conf) {
        return new ExcludeDirectoriesOrFilesFilter(conf,
                ExcludeDirectoriesOrFilesFilter.Mode.EXCLUDE_FILES);
    }

    /**
     * Composes multiple {@link org.apache.hadoop.fs.PathFilter}s. This path filter
     * accepts paths that are accepted by all of the path filters passed
     * to its constructor. It will short circuit at the first filter encountered
     * that does not accept the given path.
     */
    public static final class CompositePathFilter implements org.apache.hadoop.fs.PathFilter {
        private org.apache.hadoop.fs.PathFilter required;
        private org.apache.hadoop.fs.PathFilter[] optional;

        public CompositePathFilter(org.apache.hadoop.fs.PathFilter required, org.apache.hadoop.fs.PathFilter... optional) {
            this.required = required;
            this.optional = optional;
        }

        @Override
        public boolean accept(Path path) {
            if(required.accept(path)) {
                for (org.apache.hadoop.fs.PathFilter f : optional) {
                    if (!f.accept(path)) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
    }

    /**
     * A path filter that excludes either all files, or all directories
     * depending on whether it was constructed with EXCLUDE_DIRECTORIES or
     * EXCLUDE_FILES
     */
    private static final class ExcludeDirectoriesOrFilesFilter extends Configured implements org.apache.hadoop.fs.PathFilter {
        private static enum Mode{EXCLUDE_DIRECTORIES, EXCLUDE_FILES}
        private Mode mode;

        private ExcludeDirectoriesOrFilesFilter(){ }
        private ExcludeDirectoriesOrFilesFilter(Configuration conf){ }

        public ExcludeDirectoriesOrFilesFilter(Configuration conf, Mode mode) {
            super(conf);
            this.mode = mode;
        }

        @Override
        public boolean accept(Path path) {
            try {
                FileSystem fs = path.getFileSystem(getConf());
                FileStatus fileStatus = fs.getFileStatus(path);
                if (mode == Mode.EXCLUDE_DIRECTORIES) {
                    return !fileStatus.isDir();
                } else {
                    return fileStatus.isDir();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
