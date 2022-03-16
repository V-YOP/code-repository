package mapreduce.util;

import java.io.File;

public class Util {
    private Util(){}

    /**
     * 递归删除文件夹
     * Java原生居然不提供删除非空文件夹的方法，被迫如此
     */
    public static void deleteFile(String path) {
        if (path.startsWith("file://"))
            path = path.substring(7);
        File file = new File(path);
        if (!file.exists()) return;
        if (file.isFile()) {
            file.delete();
            return;
        }
        if (file.listFiles() == null) {
            throw new RuntimeException("It should not happen");
        }
        for (File listFile : file.listFiles()) {
            deleteFile(listFile.getAbsolutePath());
        }
        file.delete();

    }
}
