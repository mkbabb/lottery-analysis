Object.defineProperty(Array.prototype, "fill", {
    value: function(value) {
        if (this == null) {
            throw new TypeError("Array is null or undefined");
        }
        var obj = Object(this);
        var len = !obj.length ? 0 : obj.length;

        var start = arguments[1] >> 0;
        var end = arguments[2] === undefined ? len - 1 : arguments[2];

        start = start < 0 ? Math.max(start + len, 0) : Math.min(start, len);
        end = end < 0 ? Math.max(end + len, 0) : Math.min(end, len);

        do {
            obj[start] = value;
        } while (start++ < end);

        return obj;
    }
});

function transposeArray(array) {
    var shape = [array.length, array[0].length];
    var arrayT = new Array(shape[1]).fill(0);

    arrayT.forEach(function(value, i) {
        var subArray = new Array(shape[0]).fill(0);
        subArray.forEach(function(subValue, j) {
            subArray[j] = array[j][i];
        });
        arrayT[i] = subArray;
    });
    return arrayT;
}

function PFAC(N) {
    var p = 2;
    var fax = [];
    while (Math.pow(p, 2) <= N) {
        while (N % p == 0) {
            fax.push(p);
            N = Math.floor(N / p);
        }
        p += 1;
    }
    if (N != 1) {
        fax.push(N);
    }
    return fax;
}

function IS_PRIME(N) {
    return isNaN(N) ? false : PFAC(N).length == 1;
}

function SELECTED_RANGE() {
    return SpreadsheetApp.getActive()
        .getActiveRange()
        .getColumn();
}

function GET_CELL(col, row) {
    var val = col + row;
    return SpreadsheetApp.getActiveSheet().getRange(val);
}

function GET_CELL_VALUE(col, row) {
    var val = col + row;
    return SpreadsheetApp.getActiveSheet()
        .getRange(val)
        .getValue();
}

function isFloat(num) {
    return num.toString().indexOf(".") !== -1;
}

function REDUCE_IF(data, initializer, conditional, reductor) {
    initializer =
        initializer === undefined
            ? 0
            : isFloat(initializer)
            ? parseFloat(initializer)
            : parseInt(initializer);
    conditional =
        conditional === undefined
            ? function(y, i) {
                  return true;
              }
            : eval(conditional);
    reductor =
        reductor === undefined
            ? function(x, y, i) {
                  return x + y;
              }
            : eval(reductor);
    var bFloat = isFloat(data[0]);

    for (var i = 0; i < data.length; i++) {
        var val = bFloat ? parseFloat(data[i]) : parseInt(data[i]);
        val = initializer = conditional(val, i)
            ? reductor(initializer, val, i)
            : initializer;
    }
    return initializer;
}

function GET_COLOR(col, row) {
    return GET_CELL(col, row).getBackgroundColors();
}
