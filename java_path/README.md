# Java 路径操作

## 获取classpath下文件

classpath即为resources下文件，在打成jar包时，该路径下文件将会置于classes目录。

网络上关于如何获取该路径下内容有许多讨论，但似乎绝大部分都没有试图去解决打成jar包后对该路径文件进行获取的问题，现在就该问题进行求解。

这里直接给出最优解——``ClassLoader.getSystemResource(path) : URL``方法，给出一个相对路径可以直接获取到相应URL。如果没有获取到，该方法会返回``null``。下面给出返回classpath和返回classpath下文件路径的代码——

> 真正的最优解是直接使用Spring Boot提供的抽象！``classpath:``不香吗？

```java
public static Path goUp(Path ori, int time) {
    while (time-- > 0)
        ori = ori.getParent();
    return ori;
}
// 获取classpath如果是在非jar环境，可以直接使用new Object(){}.getClass().getResource("")获得，也可以通过ClassLoader.getSystemResource，用当前包的路径进行操作，获取根路径
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
```

下面进行一些实验，查看通过什么方法可能可以解决该问题——
```java
package me.littlestar;
public class TryToGetClasspath {
    public static void main(String[] args) throws Exception {
        System.out.println(Thread.currentThread().getContextClassLoader().getResource("."));
        System.out.println(new Object(){}.getClass().getResource(""));
        System.out.println(new Object(){}.getClass().getResource("."));
        System.out.println(new Object(){}.getClass().getResource("/"));
        System.out.println(new File("").getAbsolutePath());
        /*
            // 下面的结果假设在CODE目录下运行，项目文件夹为little-star，代码在me.littlestar包下运行
            // 要获取的路径对IDE来说是.../CODE/little-star/target/classes/，对jar包来说是.../CODE/little-star/target/little-star.jar!
            在IDE里运行时，结果如下：
            file:.../CODE/little-star/target/classes/
            file:.../CODE/little-star/target/classes/me/littlestar/
            file:.../CODE/little-star/target/classes/me/littlestar/
            file:.../CODE/little-star/target/classes/
            .../CODE/little-star
            打成jar包，使用java -jar命令运行时，结果如下：
            null
            jar:file:.../CODE/little-star/target/little-star.jar!/me/littlestar/
            null
            null
            .../CODE
        */
    }
}
```

``new Object(){}``的意义是为了能够获取当前Class——静态方法是不能直接拿到Class的，因此也拿不到Class所在路径，这里通过这种方法定义了一个Object的匿名实现类（它的类名是me.littlestar.Main$1）并获取其Resource，这个Resource和当前的Class的Resource是一致的。

可见，无论是在IDE中还是在jar包中，``new Object(){}.getClass().getResource("")``都能正常工作，因此应当从其着手。通过其获取classpath路径的方式是简单的——获取包名，判断包深度，向外移动该深度即可。代码见下——

```java
// 向上更改路径
public static Path goUp(Path ori, int time) {
    while (time-- > 0)
        ori = ori.getParent();
    return ori;
}
public static URL getClasspathFile(String filePath) {
    Class<?> tmpClass = new Object(){}.getClass();
    Path classpathPath = goUp(Paths.get(tmpClass.getResource("").getPath()), tmpClass.getPackage().getName().split("\\.").length); 
    Path theFilePath = classpathPath.resolve(filePath); // 拼接classpath和文件路径
    String realProtocol = tmpClass.getResource("").getProtocol().equals("jar") ? "jar:" : "file:";
    URL resultURL = new URL(realProtocol + theFilePath);
    return resultURL;
}
```

