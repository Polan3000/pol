package org.apache.hadoop.fs.qiniu.kodo;


import com.qiniu.util.Auth;
import com.qiniu.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AuthorizationException;

import java.util.Map;

public class QiniuKodoFsConfig {
    public static final String QINIU_PARAMETER_ACCESS_KEY = "fs.qiniu.access.key";

    public static final String QINIU_PARAMETER_SECRET_KEY = "fs.qiniu.secret.key";

    public static final String QINIU_PARAMETER_BUFFER_DIR_KEY = "fs.qiniu.buffer.dir";

    public static final String QINIU_PARAMETER_REGION_ID_KEY = "fs.qiniu.region.id";

    public static final String QINIU_PARAMETER_DOWNLOAD_DOMAIN_KEY = "fs.qiniu.download.domain";
    public static final String QINIU_PARAMETER_USE_HTTPS_KEY = "fs.qiniu.useHttps";
    private final Configuration conf;

    public QiniuKodoFsConfig(Configuration conf) {
        this.conf = conf;
        Map<String, String> env = System.getenv();
        conf.setIfUnset(QINIU_PARAMETER_ACCESS_KEY, env.get("QSHELL_AK"));
        conf.setIfUnset(QINIU_PARAMETER_SECRET_KEY, env.get("QSHELL_SK"));
    }


    public String getAuthAccessKey() throws AuthorizationException {
        String authAccessKey = conf.get(QINIU_PARAMETER_ACCESS_KEY);

        if (!StringUtils.isNullOrEmpty(authAccessKey)) return authAccessKey;

        throw new AuthorizationException(String.format(
                "Qiniu access key can't empty, you should set it with %s in core-site.xml",
                QINIU_PARAMETER_ACCESS_KEY
        ));
    }


    public String getAuthSecretKey() throws AuthorizationException {
        String authSecretKey = conf.get(QINIU_PARAMETER_SECRET_KEY);

        if (!StringUtils.isNullOrEmpty(authSecretKey)) return authSecretKey;

        throw new AuthorizationException(String.format(
                "Qiniu secret key can't empty, you should set it with %s in core-site.xml",
                QINIU_PARAMETER_SECRET_KEY
        ));
    }


    public Auth createAuth() throws AuthorizationException {
        Auth auth = Auth.create(getAuthAccessKey(), getAuthSecretKey());
        return auth;
    }

    /**
     * 获取bucket的region配置信息，若为空则自动获取region
     */
    public String getRegionId() {
        return conf.get(QINIU_PARAMETER_REGION_ID_KEY);
    }

    public boolean useHttps() {
        return conf.getBoolean(QINIU_PARAMETER_USE_HTTPS_KEY, true);
    }

    /**
     * 若返回空，则默认走源站
     */
    public String getDownloadDomain() {
        return conf.get(QINIU_PARAMETER_DOWNLOAD_DOMAIN_KEY);
    }

    public int getBlockSize() {
        return conf.getInt("fs.qiniu.block.size", 4 * 1024 * 1024);
    }

    public long getMultipartDownloadSize() {
        return conf.getLong("fs.qiniu.multipart.download.size", 512 * 1024);
    }

    private String bufferDir;

    public String getBufferDir() {
        if (bufferDir != null) return bufferDir;
        bufferDir = conf.get(QINIU_PARAMETER_BUFFER_DIR_KEY);
        return bufferDir;
    }
}
