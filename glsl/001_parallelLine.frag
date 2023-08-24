/*
平行线，用来画直线，考虑用来画排线，但
TODO 需要增加手绘感和随机性，以及允许做渐变
*/

#ifdef GL_ES
precision mediump float;
#endif

uniform vec2 u_resolution;
uniform vec2 u_mouse;
uniform float u_time;

// dir: 方向向量
// num: 直线数量，只有垂直和水平的时候才对得上，否则能看到的数量总是比该数量大
// offset: 偏移量
float parallelLines(vec2 dir, float num, float offset, vec2 st) {
    float x = st.x;
    float y = st.y;
    float A = dir.y;
    float B = -dir.x;
    float C = 1.;
    float dist = abs(A * x + B * y + C) / sqrt(A * A + B * B) + offset / num;
    dist = fract(dist * num);
    return smoothstep(0.4, 0.49, dist) * smoothstep(0.6, 0.51, dist);
}

void main() {
    vec2 st = gl_FragCoord.xy/u_resolution.xy;
    vec3 background = vec3(1.);
    vec3 color = background;
    color = mix(color, vec3(0.353,0.735,0.140), parallelLines(vec2(1.), 5., -0.232, st));
    color = mix(color, vec3(0.000,0.133,0.755), parallelLines(vec2(-0.410,0.660), 5., 0., st));
    gl_FragColor = vec4(color,1.0);
}