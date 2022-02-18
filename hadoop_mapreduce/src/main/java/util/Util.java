package util;

import java.io.File;

public class Util {
    private Util(){}

    public static void deleteFile(String path) {
        if (path.startsWith("file://"))
            path = path.substring(7);
        File file = new File(path);
        if (!file.exists()) return;
        if (file.isFile()) {
            file.delete();
            return;
        }
        if (file.listFiles() != null) {
            for (File listFile : file.listFiles()) {
                deleteFile(listFile.getAbsolutePath());
            }
            file.delete();
        }
    }
}
