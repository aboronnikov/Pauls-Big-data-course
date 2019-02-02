package com.epam.spark.extensions

import org.apache.hadoop.fs.{FileSystem, Path}

import scala.language.implicitConversions

/**
 * Utility class that enriches the FileSystem class with extra functionality.
 */
object FileSystemExtensions {
  /**
   * Pimp my library pattern.
   *
   * @see https://stackoverflow.com/a/3119671/10681828
   * @param fileSystem the FileSystem object being extended.
   * @return RichFileSystem object with extensions.
   */
  implicit def richFileSystem(fileSystem: FileSystem): RichFileSystem = new RichFileSystem(fileSystem)

  /**
   * Pimp my library pattern.
   *
   * @see https://stackoverflow.com/a/3119671/10681828
   * @param fileSystem the FileSystem object being extended.
   */
  class RichFileSystem(fileSystem: FileSystem) {
    /**
     * Shortcut method to check if a given path exists.
     *
     * @param basePath path to storage directory.
     * @param path     path from storage directory containing partition directories.
     * @return true or false, indicating if this path exists.
     */
    def doesPathExist(basePath: String, path: String): Boolean = {
      val pathObject = new Path(basePath + path)
      fileSystem.exists(pathObject)
    }

    /**
     * Shortcut method to remove a path if it exists.
     *
     * @param basePath path to storage directory.
     * @param path     path from storage directory containing partition directories.
     */
    private def removePathIfExists(basePath: String, path: String): Unit = if (fileSystem.doesPathExist(basePath, path)) {
      val pathObject = new Path(basePath + path)
      val isRecursive = true
      fileSystem.delete(pathObject, isRecursive)
    }

    /**
     * Removes paths on HDFS that correspond to given partitions.
     *
     * @param basePath   path to the data directory.
     * @param partitions partition paths.
     */
    def removeCorrespondingPaths(basePath: String, partitions: Array[String]): Unit = {
      partitions.foreach(path => {
        removePathIfExists(basePath, path)
      })
    }
  }

}
