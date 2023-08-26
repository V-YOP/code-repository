/*
关于随机

glsl肯定没法提供真正的随机，不纯了，对同一个输入，输出总是相同的。随机通过三角函数和frag实现
*/

#ifdef GL_ES
precision mediump float;
#endif

uniform vec2 u_resolution;
uniform vec2 u_mouse;
uniform float u_time;

float random (vec2 st) {
    return fract(sin(dot(st.xy, vec2(12.9898,78.233)))* 4358.5453123);
}

void main() {
    vec2 st = gl_FragCoord.xy/u_resolution.xy;
    // 默认情况下，每个像素都有自己的随机数
    // 而使用floor能够生成块状的随机
    st = floor(st * 50.); // 50 x 50 的随机（跟基岩似的）
    gl_FragColor = vec4(vec3(random(st)),1.0);
}