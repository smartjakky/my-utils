from pyecharts.charts.pie import Pie
from pyecharts.engine import EchartsEnvironment


class PdbEchartsEnvironment(EchartsEnvironment):
    def render_chart_to_html(self, template_name, **kwargs):
        tpl = self.get_template(template_name)
        return tpl.render(**kwargs)

class PdbPie(Pie):
    viz_type = 'pie'
    def render_to_html(
        self,
        template_name="chart.html",
        object_name="chart",
        **kwargs
    ):
        env = PdbEchartsEnvironment()
        html = env.render_chart_to_html(
            chart=self,
            object_name=object_name,
            template_name=template_name,
            **kwargs
        )
        return html

p = PdbPie("饼图", "例子")
p.add("alpha", ["a", "b", "c"], [1,2,3])
p.show_config()
p.print_echarts_options()
html = p.render_to_html(template_name="chart.html")

# print(html)
