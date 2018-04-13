var _date="19/09/2013".split(/\//)

//var initial = _date.split(/\//).reverse().join('/');
var initial=[ _date[1], _date[0], _date[2] ].join('/')
console.log(initial)

//var n = _date.toISOString();

var d=new Date(initial)

console.log(d.toUTCString())
//.toUTCString()


var _date1="10/10/1949  20:30:00"

var _date1_asDate=new Date(_date1)
console.log(_date1_asDate.toUTCString())
var ts = Math.round((_date1_asDate).getTime() / 1000);
console.log(ts)

//console.log(_date1_asDate.tou)