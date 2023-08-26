/**

下面建立的心智模型还是不对……想象每次变换都是对原坐标系创建一个“视图”坐标系（就像数组的视图或者数据库的视图），从原坐标系的角度来看，平移会移动视图坐标系的原点，缩放会在不移动视图原点的情况下调整视图坐标系单位长度，旋转会不移动视图原点的情况下旋转视图。始终以原坐标系为锚点。视图在其原点处进行绘制时，原坐标系下视图相应位置处会被绘制。显然，视图坐标系到原坐标系的位置关系是必须的。

这样就能解释平移和旋转的顺序不同时作用不同的机理了。先平移再旋转时，视图从原点出发，先向前走10步，然后随时间旋转自身，在旋转的过程中，视图原点的位置是不动的，因此在视图原点处绘制的物体也是不动的。先旋转再平移时，视图对每一个角度，都要向前走10步并在视图原点绘制形状，显然对每一个角度，视图原点的位置都是不一样的，因此形状在原坐标系中绘制的位置也会有变化。

以镜头为心智模型的话，平移和缩放能够解释，但若物体本身不在画面中间，旋转是难以解释的，强行说画面只是镜头的一部分也并不直观。

---

2d变换，包括平移，缩放，旋转，斜切，镜像等，实现了这些函数，之后在实现任何形状时，只需要绘制原点上的“单位”形状，再对其应用变换即可。

如何理解变换？想象镜头垂直向下看一张（无穷大的）平面图片，进行2d变换，就是（在平行于该平面的平面上）移动和滚转镜头，但原图片是不变的。
（这个心智模型对于3d变换似乎也适用，变的是镜头，世界还是那个世界，所谓相机参照系和世界参照系，但现象上是世界变了

我们操作的st是镜头坐标系，前面的学习里一直当作世界坐标系，是因为它们两个刚好一致罢了；绘图始终在世界坐标系中进行

世界坐标系其实也是相对的，书中有写 st = st * 2. - 1.，将世界坐标系的值域转换为-1,1，只要在后面一直使用转换后的坐标，那它相对来说就是绝对的了

*/
#ifdef GL_ES
precision mediump float;
#endif
uniform vec2 u_resolution;
uniform float u_time;
#define PI 3.14159265359

// 长宽为1，边平行于坐标轴，中心在原点的正方形，测试用
float box(vec2 st) {
    vec2 bl = smoothstep(-0.5, -0.5+.01, st);
    vec2 tr = smoothstep(0.5, 0.5-.01, st);
    return bl.x * bl.y * tr.x * tr.y;
}

// 首先是平移
// 想象当前有个形状在画面中间，即镜头正下方；向左下角移动镜头，该形状会向右上角走。
// 因此，要让形状往某个方向走，只需要让镜头向该方向的反方向移动即可。
// dir是形状移动的方向，st是镜头坐标系下的点，返回世界坐标系下的点
// 比如平移(1, 2)，此时镜头平移(-1, -2)，世界原点的位置就在镜头坐标系下(1, 2)的位置
// 注意这里的dir的“量纲”是镜头坐标系，如果镜头已经放大过，在世界坐标系上的移动是会变大的
vec2 translate(vec2 dir, vec2 st) {
    return st - dir;
}

// 然后是缩放
// 对镜头来说，缩放就是变焦，要把镜头下面的内容放大，只需要变焦即可
// 对焦距的另一个心智模型是，长焦是从短焦的画面中剪切中央部分的结果，比如，短焦时，镜头能看到世界坐标系中(-100, 100)的内容，
// 换成长焦，就只能看到(-5, 5)的内容，然后，我们把这(-100, 100)，(-5, 5)的内容映射到镜头画面中，显然后者的缩放是更大的
// 回到正题，初始时，我们把(0, 1)的世界坐标系映射到镜头坐标系中的(0, 1)，我们只需要修改为把(0, 0.5)的世界坐标系映射到镜头坐标系中
// 的(0, 1)，便得到了缩放2倍的效果
// ratio是x，y轴的缩放倍率，st是镜头坐标系上的点，返回对应的世界坐标系上的点
// 缩放2倍时，对镜头坐标系上的点(0, 1)，会得到世界坐标系上的点(0, 0.5)，对 (0, 0.5)，得到 (0, 0.25)
// 显然，这里是除法
// 注意缩放后再做平移的话，dir的大小也是会被缩放的
vec2 scale(vec2 ratio, vec2 st) {
    return st / ratio;
}


// 演示缩放和平移
void translateAndScaleExample0(out vec4 glFragColor, in vec2 st) {
    // 如果交换这两行，会发现translate里要移动(1, 1)才能将正方形移至正中心
    // 此时移动(1, 1)在世界坐标系上移动的(.5, .5)

    st = translate(vec2(.5), st); // 形状移动(.5, .5)
    st = scale(vec2(.5), st); // 形状长宽变为1/2
    glFragColor = vec4(vec3(box(st)), 1.);
}

// 另一个例子，镜头围绕物体移动
void translateAndScaleExample1(out vec4 glFragColor, in float time, in vec2 st) {
    // 如何让镜头围绕物体移动呢？考虑sin函数，sin(x)^2 + cos(x)^2 = 1，正好是单位圆
    // 但它们的值域为-1，1，这里将它们先变换为0，1
    vec2 trans = vec2(sin(time), cos(time)) * .6; // 减小半径，始终处在画面中
    trans = (trans + 1.) / 2.;
    st = translate(trans, st); 
    // 给这个"pipeline"加个分支，现在st1和st2是两个不同的镜头坐标系了，来画个十字
    vec2 st1 = scale(vec2(.1, .2), st); 
    vec2 st2 = scale(vec2(.2, .1), st); 
    // 相加即做并集，相乘即做交集
    glFragColor = vec4(vec3(box(st1) + box(st2)), 1.);
}

// 旋转，这个有难度了
// 这个怎么解释……
vec2 rotate(float angle, vec2 st) {
    return mat2(cos(angle),-sin(angle),
                sin(angle),cos(angle)) * st;
}

void rotateExample(out vec4 glFragColor, in float time, in vec2 st) {
    st = translate(vec2(.5), st); // 形状移动(.5, .5)
    st = scale(vec2(.5), st); // 形状长宽变为1/2
    st = rotate(time, st);
    glFragColor = vec4(vec3(box(st)), 1.);
}

void main() {
    vec2 st = gl_FragCoord.xy/u_resolution;

    // 缩放和平移例子
    translateAndScaleExample0(gl_FragColor, st);
    translateAndScaleExample1(gl_FragColor, u_time, st);
    rotateExample(gl_FragColor, u_time, st);
    
}
