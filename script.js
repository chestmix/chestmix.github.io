const canvas = document.createElement("canvas");
const ctx = canvas.getContext("2d");
document.body.appendChild(canvas);
canvas.width = 600;
canvas.height = window.innerHeight;
//canvas commands  https://www.w3schools.com/tags/ref_canvas.asp
//example drawings

let lineW = 2;
let iNum = 1;
let funct = 0;
let area = 0;

document.getElementById("button1").addEventListener("click", function xby2() {
  funct = 1;
});

document
  .getElementById("button2")
  .addEventListener("click", function xSquared() {
    funct = 2;
  });
let subDiv = 0;
document
  .getElementById("subdiv")
  .addEventListener("click", function subdivNum() {
    subDiv = prompt("Enter amount of Sub-Divisions", "0") * 1;
    console.log(subDiv);
  });

let lmr = 0;
document
  .getElementById("leftPoint")
  .addEventListener("click", function leftCheck() {
    lmr = 1;
  });

document
  .getElementById("middlePoint")
  .addEventListener("click", function middleCheck() {
    lmr = 2;
  });

document
  .getElementById("rightPoint")
  .addEventListener("click", function rightCheck() {
    lmr = 3;
  });

//Cutsom Setup
let x = [];
let y = [];
let numVal = 0;
let draw = 0;
document
  .getElementById("custom")
  .addEventListener("click", function inputValues() {
    funct = 0;
    numVal = prompt("How many points would you like? (Max 10)");
    if (numVal <= 10) {
      for (let i = 0; i < numVal; i++) {
        x[i] = prompt("Enter X-value " + (i + 1) + ":") * 1;
        if (x[i] <= x[i - 1] && i != 0) {
          alert("x values must go up in value");
          i = numVal;
          draw = 0;
        } else {
          draw = 1;
        }
        y[i] = prompt("Enter Y-value " + (i + 1) + ":") * 1;
      }
    }
  });
console.log(x);
console.log(y);

// ___________________animation loop ___________________

function cycle() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "white";
  ctx.strokeStyle = "white";

  //graph

  ctx.fillStyle = "white";
  ctx.strokeStyle = "white";
  for (let i = 0; i <= 500; i += 50) {
    ctx.beginPath();
    ctx.fillRect(i, 500, lineW, -canvas.height);
    ctx.fill();
    ctx.stroke();
  }
  for (let i = 0; i <= 500; i += 50) {
    ctx.beginPath();
    ctx.fillRect(0, i, 500, lineW);
    ctx.fill();
    ctx.stroke();
  }

  //function
  if (funct == 1) {
    ctx.beginPath();
    ctx.moveTo(0, 500);
    ctx.lineTo(500, 250);
    ctx.fillStyle = "white";
    ctx.strokeStyle = "white";
    ctx.fill();
    ctx.lineWidth = 5;
    ctx.stroke();

    if (lmr == 1) {
      ctx.fillStyle = "red";
      ctx.strokeStyle = "red";
      ctx.globalAlpha = 0.5;
      for (let i = 0; i < 499; i += 500 / subDiv) {
        ctx.fillRect(i, 500 - i / 2, 500 / subDiv, i / 2);
      }
      if (subDiv != 0) {
        area = 25 - 25 / subDiv;
      } else {
        area = 0;
      }
    } else if (lmr == 2) {
      ctx.fillStyle = "red";
      ctx.strokeStyle = "red";
      ctx.globalAlpha = 0.5;
      for (let i = 0; i < 499; i += 500 / subDiv) {
        ctx.fillRect(
          i,
          500 - i / 2 - 125 / subDiv,
          500 / subDiv,
          i / 2 + 125 / subDiv
        );
      }
      if (subDiv != 0) {
        area = 25;
      } else {
        area = 0;
      }
    } else if (lmr == 3) {
      ctx.fillStyle = "red";
      ctx.strokeStyle = "red";
      ctx.globalAlpha = 0.5;
      for (let i = 0; i < 501; i += 500 / subDiv) {
        ctx.fillRect(i - 500 / subDiv, 500 - i / 2, 500 / subDiv, i / 2);
      }
      if (subDiv != 0) {
        area = 25 + 25 / subDiv;
      } else {
        area = 0;
      }
    }

    ctx.globalAlpha = 1;
  } else if (funct == 2) {
    ctx.beginPath();
    for (let i = 0; i < 500; i++) {
      ctx.moveTo(i, 500 - (i * i) / 50);
      ctx.lineTo(i + 1, 500 - ((i + 1) * (i + 1)) / 50);
    }
    ctx.fillStyle = "white";
    ctx.strokeStyle = "white";
    ctx.fill();
    ctx.lineWidth = 5;
    ctx.stroke();
  } else if (funct == 0) {
    ctx.beginPath();

    ctx.fillStyle = "white";
    ctx.strokeStyle = "white";
    ctx.lineWidth = 5;
    for (let i = 0; i < numVal; i++) {
      ctx.moveTo(x[i] * 50, (10 - y[i]) * 50);
      ctx.lineTo(x[i + 1] * 50, (10 - y[i + 1]) * 50);
    }
    ctx.fill();
    ctx.stroke();
    
    if(lmr == 1){
      ctx.fillStyle = "red";
      ctx.strokeStyle = "red";
      ctx.globalAlpha = 0.5;
      area = 0
//---------------NEEDS WORK-----------------\\ 
     for (let i = 0; i < x.length-1; i++){
       ctx.fillRect((x[i]*50), (500 - (y[i]*50)), ((x[(i+1)]*50) - (x[i]*50)), (y[i]*50));
       area += (x[(i+1)]-x[i]) * y[i]
     }
     
    }
    
    if(lmr == 2){
      ctx.fillStyle = "red";
      ctx.strokeStyle = "red";
      ctx.globalAlpha = 0.5;
      area = 0
//---------------NEEDS WORK-----------------\\ 
     for (let i = 0; i < numVal; i++){
       ctx.fillRect((x[i]*50), (500 - (y[i]*50)), ((x[(i+1)]*50) - (x[i]*50)), (y[i]*50))
     }
     
    }
    ctx.globalAlpha = 1;
  }
//----------------------------------------------\\
  //area

  ctx.font = "30px arial";
  ctx.textAlign = "center";
  ctx.fillStyle = "white";
  ctx.fillText("Estimate Area: " + area, canvas.width / 2, 600);

  //bottom line
  ctx.fillStyle = "white";
  ctx.strokeStyle = "white";
  ctx.fillRect(0, 500, canvas.width, 10);

  requestAnimationFrame(cycle);
}
requestAnimationFrame(cycle);
