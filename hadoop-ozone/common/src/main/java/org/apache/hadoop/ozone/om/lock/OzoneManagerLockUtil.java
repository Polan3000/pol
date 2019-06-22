package org.apache.hadoop.ozone.om.lock;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_SECRET;
import static org.apache.hadoop.ozone.OzoneConsts.OM_USER_PREFIX;
import static org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy.LOG;

/**
 * Utility class contains helper functions required for OM lock.
 */
public final class OzoneManagerLockUtil {


  private OzoneManagerLockUtil() {
  }

  /**
   * Generate resource lock name for the given resource name.
   *
   * @param resource
   * @param resourceName
   */
  public static String generateResourceLockName(
      OzoneManagerLock.Resource resource, String resourceName) {

    if (resource == OzoneManagerLock.Resource.S3_BUCKET) {
      return OM_S3_PREFIX + resourceName;
    } else if (resource == OzoneManagerLock.Resource.VOLUME) {
      return OM_KEY_PREFIX + resourceName;
    } else if (resource == OzoneManagerLock.Resource.USER) {
      return OM_USER_PREFIX + resourceName;
    } else if (resource == OzoneManagerLock.Resource.S3_SECRET) {
      return OM_S3_SECRET + resourceName;
    } else if (resource == OzoneManagerLock.Resource.PREFIX) {
      return OM_S3_PREFIX + resourceName;
    } else {
        throw new IllegalArgumentException("Unidentified resource type is" +
            " passed when Resource type is bucket");
    }

  }

  /**
   * Generate bucket lock name.
   * @param volumeName
   * @param bucketName
   */
  public static String generateBucketLockName(String volumeName,
      String bucketName) {
    return OM_KEY_PREFIX + volumeName + OM_KEY_PREFIX + bucketName;

  }

}
