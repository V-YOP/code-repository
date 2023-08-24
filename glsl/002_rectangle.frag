#ifdef GL_ES
precision mediump float;
#endif

uniform vec2 u_resolution;

// 使用floor实现step，just for practice
vec2 myStep(vec2 threshold, vec2 st) {
    return floor(st - threshold) + 1.;
}

float rectangle(vec2 start, vec2 end, vec2 st) {
	vec2 realStart = vec2(min(start.x, end.x), min(start.y, end.y)); // 找到实际左下角的点
    vec2 realEnd = vec2(max(start.x, end.x), max(start.y, end.y)); // 找到实际右上角的点
    vec2 bl = step(realStart, st); // bottom left, 在起始点的左边时，x=0，否则x=1；在起始点点下边时，y=0，否则y=1
	vec2 tr = 1. - step(realEnd, st); // top right, 在终止点的右边时，x=0，否则x=1，在终止点上面时，y=0，否则y=1
    return bl.x * bl.y * tr.x * tr.y;
}

// 画一幅抽象画
void main(){
    vec2 st = gl_FragCoord.xy/u_resolution.xy;
    vec3 background = vec3(247., 239., 220.) / 255.;
    vec3 red = vec3(170., 40., 40.) / 255.;
    vec3 orange = vec3(250., 195., 68.) / 255.;
    vec3 skyBlue = vec3(17., 83., 140.) / 255.;
    vec3 black = vec3(19., 19., 24.) / 255.;
    
    vec3 color = background;
    // 色块
    color = mix(color, red, rectangle(vec2(0.,1.), vec2(0.260,0.590), st));
    color = mix(color, orange, rectangle(vec2(0.920,0.590), vec2(1.), st));
    color = mix(color, skyBlue, rectangle(vec2(0.730,-1.000), vec2(1.000,0.120), st));   
 
    // 竖线
    color = mix(color, black, rectangle(vec2(0.220,0.000), vec2(0.260,1.0), st));
    color = mix(color, black, rectangle(vec2(0.730,-0.010), vec2(0.77,1.0), st));
    color = mix(color, black, rectangle(vec2(0.910,-0.020), vec2(0.95,1.0), st));
    
    // 横线
    color = mix(color, black, rectangle(vec2(0.,0.590), vec2(1.000,0.640), st));
    color = mix(color, black, rectangle(vec2(0., 0.790), vec2(1.000,0.840), st));
    color = mix(color, black, rectangle(vec2(0.220,0.090), vec2(1.000,0.140), st));

    gl_FragColor = vec4(color,1.0);
}
