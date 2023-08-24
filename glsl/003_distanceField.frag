/**
关于距离场的学习

距离场是画面上任意一点同特定点的距离相关联的场，距离使用亮度来表示，距离越远，亮度越大。距离场并不是特定的几何图形，它是无穷大的，通过距离场来绘制各种东西是把它当作工具，而不是绘制它本身；距离场的用途：

1. 绘制硬边和软边圆形
2. 绘制对称圆形，矩形，四角星
3. 为上述的形状描边
4. ...
*/
#ifdef GL_ES
precision mediump float;
#endif
uniform vec2 u_resolution;

// 普通画圆法，并非距离场，距离场是无限的
// c：圆心，r：半径，在圆内时返回1，否则返回0
float circle(vec2 c, float r, vec2 st) {
    // 换成smoothstep就能做圆形渐变，非常soft的渐变
    return 1. - step(r, distance(c, st)); // 避免使用sqrt能提高性能，但我又不是做游戏，不需要考虑性能
}

void main(){
  vec2 st = gl_FragCoord.xy/u_resolution.xy;
  st.x *= u_resolution.x/u_resolution.y;
  vec3 color = vec3(0.0);
  float d = 0.0;

  // 把坐标系转换为-1,1，原点为0,0
  st = st *2.-1.;

  // 做到原点的距离场（这书给的示例实在太少了！）
  d = length(st);
  // 各种奇怪的距离场，可以把它们考虑成先对st做某种变换，再求变换后的点到原点的距离并展示
  // 对每个st，向左下移动0.3再求到原点的距离，这样，原点处的值为0.3*sqrt(2)，0.3处的值为0，即最暗；相当于整个厂向右上移动了0.3
  // d = length(st -.3 ); 

  // 对st取绝对值并向左下移动0.3，相当于对上一个例子，把第一象限的内容对称变换到其他象限
  // d = length( abs(st)-.3 ); 

  // 在第一象限时，始终为 0，第三象限时正常做圆渐变，第二象限时，只和x坐标相关（y恒为0），第四象限时，只和y坐标相关（x恒为0）（注意这种性质能造成直线
  // d = length(min(st, 0.)); 

  // min沿y=-x的轴对称
  // d = length(max(st, 0.)); 

  // 在第一象限时，始终为 0，把第一象限的内容对称变换到其他象限，我是说，全为 0
  // d = length(min(abs(st), 0.));  

  // 在第一象限时，正常做圆渐变，把第一象限的内容对称变换到其他象限，我是说，和length(st)一致
  // d = length(max(abs(st), 0.));  
  
  // 假设0.3, 0.3为圆心，在第三象限，正常做圆渐变，第一象限全为 0，第二象限只和x相关，第四象限只和y相关
  // d = length( min(st-.3,0.) );

  // 对上一个例子，把第一象限的内容对称变换到其他象限，形状类似一个十字
  // d = length( min(abs(st)-.3,0.) );
  
  // 假设0.3, 0.3为圆心，在第一象限，正常做圆渐变，第三象限全为 0，第二象限只和y相关，第四象限只和x相关
  // d = length( max(st-.3,0.) );

  // 对上一个例子，把第一象限的内容对称变换到其他象限，形状类似矩形
  // d = length( max(abs(st)-.3,0.) );

  // 可视化距离场，这个直接用距离来作为亮度，可以用来debug
  gl_FragColor = vec4(vec3(d),1.0);

  // 其他可视化方法
  // 使用fract做出同心圆（漏斗）效果
  // gl_FragColor = vec4(vec3(fract(d * 10.0)),1.0);

  // 绘制硬边图像
  // gl_FragColor = vec4(vec3( step(.3,d) ),1.0);

  // 描边，利用两个step相乘造成一个“脉冲”的函数
  // gl_FragColor = vec4(vec3( step(.3,d) * step(d,.4)),1.0);

  // 同上，但软边缘
  // gl_FragColor = vec4(vec3( smoothstep(.3,.4,d)* smoothstep(.6,.5,d)) ,1.0);
}
