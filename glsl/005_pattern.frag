/**
将st缩小n倍（即st = st * n）后做一个fract，能得到一个把图像拷贝n*n次的视图，在视图中的0，1去绘制时，能在这n^2个子图像中看到同样结果
*/


#ifdef GL_ES
precision mediump float;
#endif
uniform vec2 u_resolution;
uniform float u_time;
#define PI 3.14159265359

vec2 translate(vec2 dir, vec2 st) {
    return st - dir;
}
vec2 scale(vec2 ratio, vec2 st) {
    return st / ratio;
}
vec2 rotate(float angle, vec2 st) {
    return mat2(cos(angle),-sin(angle),
                sin(angle),cos(angle)) * st;
}

// 长宽为1，边平行于坐标轴，中心在原点的正方形，测试用
float box(vec2 st) {
    vec2 bl = smoothstep(-0.5, -0.5+.01, st);
    vec2 tr = smoothstep(0.5, 0.5-.01, st);
    return bl.x * bl.y * tr.x * tr.y;
}
// 黑 白
// 白 黑
float chessboardPart(vec2 st) {
    vec2 fst = translate(vec2(.25), st);
    fst = scale(vec2(.5), fst);
    vec2 snd = translate(vec2(.75), st);
    snd = scale(vec2(.5), snd);
    return box(fst) + box(snd);
}

vec2 tile(float zoom, vec2 st) {
    return fract(st * zoom);
}

// 展示tile的用法
void example0(out vec4 glFragColor, vec2 st) {
    st = tile(10., st);
    glFragColor = vec4(vec3(box(st)), 1.);
}

// 通过矩阵变换操作每个子空间
void example1(out vec4 glFragColor, float time, vec2 st) {
    // 取消注释这两行，看世界旋转
    // st = translate(vec2(.5), st);
    // st = rotate(time, st);

    st = tile(10., st); // 创建子空间
    // 对每个子空间
    st = translate(vec2(.5), st);
    st = scale(vec2(sqrt(2.) / 2.), st);
    st = rotate(time, st);
    glFragColor = vec4(vec3(box(st)), 1.);
}

// 对子网格中的奇偶行进行不同的变换，就像砖墙上的砖块或者地砖
// 要知道当前是奇数行还是偶数行，需要处理fract之前的st，首先mod(2.)，然后再检查其中小于1.0的即为奇数行
void example2(out vec4 glFragColor, float time, vec2 st) {
    st /= vec2(2.15,0.65)/1.5; // 调整一下st的比例
    float TILE_NUM = 5.;
    float isOdd = step(mod(st.y * TILE_NUM, 2.0), 1.); // 奇数时为1，偶数时为0
    // 需要在fract之前进行偏移，注意偏移量大小要和子空间数量相关，因为子空间数量和子空间大小相关
    st = translate(vec2(isOdd * .5 / TILE_NUM, .0), st);
    // 大家都来动一动！
    // st = translate(vec2(((isOdd * 2.) - 1.) * time, .0), st);

    st = tile(TILE_NUM, st);
    st = translate(vec2(.5), st);
    st = scale(vec2(.9), st);
    glFragColor = vec4(vec3(box(st)), 1.);
}

// https://thebookofshaders.com/edit.php#09/marching_dots.frag
void example3(out vec4 glFragColor, float time, vec2 st) {
    float TILE_NUM = 10.;
    // 能发现，这张图有两种运动模式，因此需要分类讨论
    float mode = step(1., mod(time, 2.)); // 第一种模式，定义其为x轴运动，返回0，否则返回1，对应第二种模式
    // 同样需要讨论为奇数还是偶数，这里要同时适应mode为0和1的情况，为0时研究y轴，为1时研究x轴
    float isOdd = step(mod(st.y * TILE_NUM * (1. - mode) + st.x * TILE_NUM * mode, 2.0), 1.);
    st = translate(vec2(((isOdd * 2.) - 1.) * time) * vec2(1. - mode, mode) / TILE_NUM, st);
    st = tile(TILE_NUM, st);
    st = translate(vec2(.5), st);
    st = scale(vec2(.5), st);
    glFragColor = vec4(vec3(box(st)), 1.);
}

// 一个渐变圆，通过黑白比例而非灰度去控制亮度
void example4(out vec4 glFragColor, float time, vec2 st) {
    float TILE_NUM = 23.;
    // 视图移动到中心
    st = translate(vec2(0.5), st);
    st = rotate(time, st);
    // 视图再移动到第一个子空间的中心（不如此的话旋转会导致沿第一个子空间的左下角位置旋转，而不是沿第一个子空间中心旋转）
    // 因为子空间绘图的时候并非以子空间中心为中心
    st = translate(vec2(- 1. / TILE_NUM / 2.), st);
    // 计算当前的子空间到中心的距离，要求子空间中每一个点得到的距离都是相同的
    float tileDistance = length(floor(st * TILE_NUM))/ TILE_NUM;
    
    st = tile(TILE_NUM, st);
    
    // 对每个子空间
    st = translate(vec2(.5), st);
    st = scale(vec2(clamp(0., 1.,1. - tileDistance * 2.)), st);
    glFragColor = vec4(vec3(1. - box(st)), 1.);
}


void main() {
    vec2 st = gl_FragCoord.xy/u_resolution;
    example0(gl_FragColor, st);
    example1(gl_FragColor, u_time, st);
    example2(gl_FragColor, u_time, st);
    example3(gl_FragColor, u_time, st);
    example4(gl_FragColor, u_time, st);
}