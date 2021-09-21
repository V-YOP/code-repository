public class Classpath {
    public static URL getClasspathURL() {
        try {
            Class<?> tmpClass = new Object(){}.getClass();
            Path classpathPath = goUp(Paths.get(tmpClass.getResource("").getPath()), tmpClass.getPackage().getName().split("\\.").length);
            String realProtocol = tmpClass.getResource("").getProtocol().equals("jar") ? "jar:" : "file:";
            return new URL(realProtocol + classpathPath + "/"); // 不加斜杠的话在jar包环境下会报错说URL不合法
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public static Optional<URL> getClasspathFileURL(String filePath) {
        return Optional.ofNullable(ClassLoader.getSystemResource(filePath));
    }
}