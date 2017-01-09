# 標高・傾斜度・土地利用メッシュデータ

## 権利者情報

国土数値情報

## 加工者

三好 邦彦
contact@colspan.net
http://github.com/colspan

## 入手方法

下記URLより手動ダウンロード
http://nlftp.mlit.go.jp/ksj/index.html

### 国土数値情報　標高・傾斜度3次メッシュデータ
http://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-G04-a.html

### 国土数値情報　土地利用3次メッシュデータ
http://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-L03-a.html


### 対象1次メッシュコード
  - 6239
  - 6240
  - 6241
  - 6243
  - 6339
  - 6340
  - 6341
  - 6342
  - 6343
  - 6439
  - 6440
  - 6441
  - 6442
  - 6443
  - 6444
  - 6445
  - 6540
  - 6541
  - 6542
  - 6543
  - 6544
  - 6545
  - 6641
  - 6642
  - 6643
  - 6644
  - 6645
  - 6741
  - 6742
  - 6840
  - 6841
  - 6842

```javascript
/* 選択画面 */
var target_meshids = ["6239", "6240", "6241", "6243", "6339", "6340", "6341", "6342", "6343", "6439", "6440", "6441", "6442", "6443", "6444", "6445", "6540", "6541", "6542", "6543", "6544", "6545", "6641", "6642", "6643", "6644", "6645", "6741", "6742", "6840", "6841", "6842"];
var option_elements = document.getElementsByTagName('option');
Array.prototype.forEach.call(option_elements, function(o){
    o.selected = target_meshids.includes(o.value);
});
document.getElementsByTagName('input')[2].click();

/* ダウンロード対象選択画面 */
// defaultValue : G04-a-11_([0-9]+)-jgd_GML
var select_elements = document.getElementsByTagName('input');
Array.prototype.forEach.call(select_elements, function(s){
    s.checked = s.defaultValue.match(/G04-a-11_([0-9]+)-jgd_GML/ig);
    if(s.defaultValue == "　　次　へ　　"){
        s.click();
    }
});


/* ダウンロード対象選択画面 */
// defaultValue : L03-a-14_([0-9]+)-jgd_GML.zip
var select_elements = document.getElementsByTagName('input');
Array.prototype.forEach.call(select_elements, function(s){
    s.checked = s.defaultValue.match(/L03-a-14_[0-9]+-jgd_GML/ig);
    if(s.defaultValue == "　　次　へ　　"){
        s.click();
    }
});

/* ダウンロードボタン連投画面 */
function DownLd(size,file, path, mes){
    mes.value = "ダウンロード済み";
    document.location.href = path;
}
var select_elements = document.getElementsByTagName('input');
var f = (function(x){
    var i=0;
    var f = function(){
        if(i<select_elements.length){
            var s = select_elements[i];
            if(s.defaultValue == "　ダウンロード　"){
                s.click();
            }
            i++;
            setTimeout(f,500);
        }
    }
    return f;
})(select_elements);
f();

```