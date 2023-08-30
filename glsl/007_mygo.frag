
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
void draw(inout vec3 background, vec3 color, float pct) {
    background = mix(background, color, pct);
}

float shape0(vec2 st) {
    if (st.x < 0. || st.y < 0.) return 0.;
    float a = 2.414 * st.x - 0.589;
    return clamp(0., 1., sign(st.y - a) - sign(st.y - st.x));
}
float dist2line(float k, float b, vec2 st) {
    float x = st.x;
    float y = st.y;
    float A = k;
    float B = -1.;
    float C = b;
    float dist = abs(A * x + B * y + C) / sqrt(A * A + B * B) ;
    return dist;
}
float belowLine(float k, float b, vec2 st) {
    return step(st.y - k * st.x - b, 0.);
}
float lb(vec2 x, vec2 st) {
    vec2 r = step(st, vec2(x));
    return r.x * r.y;
}
float drawPart(vec2 st) {
    float test;
    test += (step(0.691, length(st)) * step(length(st), 0.8333));
    test = clamp(0., 1., test);
    test += belowLine(-15. * PI / 180., 0.2557, st);
    test = clamp(0., 1., test);
    test = clamp(0., 1., clamp(0., 1., belowLine(-15. * PI / 180., 0.2557, vec2(st.y, st.x))) + test);
    test = clamp(0., 1., test);
    test += shape0(st) + shape0(vec2(st.y, st.x));
    test = clamp(0., 1., test);
    test -= clamp(0., 1., belowLine(-15. * PI / 180., 0.2083, vec2(st.x, st.y)));
    test = clamp(0., 1., test);
    test += belowLine(60. * PI / 180., 0., vec2(st.y, st.x)) * lb(vec2(.24), st);
    test = clamp(0., 1., test);
    return st.x < 0. || st.y < 0. ? 0. : test;
}
float myGo(vec2 st) {
    float res;
    res += drawPart(st);
    st = rotate(90. * PI / 180., st);
    res += drawPart(st);
    st = rotate(90. * PI / 180., st);
    res += drawPart(st);
    st = rotate(90. * PI / 180., st);
    res += drawPart(st);
    st = rotate(90. * PI / 180., st);
    return res;
}
void main() {
	vec2 st = gl_FragCoord.xy/u_resolution;
    vec3 blue = vec3(51., 124., 175.) / 255.;
    st = st * 2. - 1.;
    st = rotate(u_time, st);
    vec3 c = blue;
    draw(c, vec3(1.), myGo(st));
	gl_FragColor = vec4(c,1.0);
}

