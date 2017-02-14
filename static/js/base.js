
var loading = '<p style="text-align:center"><img src="/static/css/loading.gif"/></p>'

Highcharts.setOptions({
    global: {
        useUTC: false
    }
});

function daterange(pickerId, cb){
    // 日期选择器

    $("#"+pickerId).daterangepicker(
        {
            timePicker: true,
            timePicker12Hour: false,
            ranges: {
                "最近 30 分钟": [moment().subtract(30, "m").startOf("s"), moment().startOf("s")],
                "最近 60 分钟": [moment().subtract(1, "h"), moment()],
                "最近 3 小时": [moment().subtract(3, "h"), moment()],
                "最近 6 小时": [moment().subtract(6, "h"), moment()],
                "最近 12 小时": [moment().subtract(12, "h"), moment()],
                "昨天": [moment().subtract(1, "d").startOf("d"), moment().subtract(1, "d").endOf("d")],
                "最近 3 天": [moment().subtract(3, "d"), moment()],
                "最近 7 天": [moment().subtract(7, "d"), moment()]
            },
            startDate: moment().subtract(30, "m").startOf("s"),
            endDate: moment().startOf("s")
        },
        cb
    );
}

function makeChart(chartId, text, series) {
    // 生成highcharts图表

    var chart = new Highcharts.Chart({
        chart: {
            renderTo: chartId,
            zoomType: "x",
            type: "spline"
        },
        title: {
            text: text
        },
        series: series,
        xAxis: {  
            type: "datetime",  
            labels: {
                formatter: function () {  
                    return Highcharts.dateFormat("%H:%M", this.value);  
                }
            }  
        },  
        credits: {
            enabled: false
        }
    })

    // 隐藏/显示 highcharts图例
    $("#"+chartId).append('<button type="button" class="btn btn-primary btn-xs update-legend" data-show="false">隐藏图例</button>')
        .find('.update-legend')
        .click(function(){
            var show = $(this).data('show');
            var showText = show?'隐藏图例':'显示图例';

            $(this).data('show',!show).html(showText);

            chart.series.forEach(function(serie){
                serie.update({
                    visible: show
                }, false)
            });

            chart.redraw();
        });
}