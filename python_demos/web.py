import os
import json

from werkzeug.serving import run_simple
from werkzeug.wrappers import Response, Request
from werkzeug.wsgi import SharedDataMiddleware
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import HTTPException
from redis.client import Redis

from jinja2 import Environment, FileSystemLoader


def env_to_dict(env):
    for key, value in env.items():
        env[key] = str(value)
    return env


class Flask(object):
    def __init__(self, file, static_folder='static', static_url='/static', template_path='templates'):
        self.redis = Redis(host='localhost')
        self.static_folder = os.path.join(os.path.abspath(file), static_folder)
        self.static_url = static_url
        self.jinja_env = Environment(loader=FileSystemLoader(template_path), autoescape=True)
        self.url_map = Map([
            Rule('/', endpoint='welcome'),
            Rule('/a', endpoint='a'),
            Rule('/b', endpoint='b'),
            Rule('/c', endpoint='c'),
            Rule(static_url, endpoint='file_system')
        ])

    def dispatch_request(self, request):
        # print(str(self.redis))
        a = self.url_map.bind_to_environ(request.environ)
        endpoint, value = a.match()
        try:
            func = getattr(self, endpoint)
        except HTTPException as e:
            return(e)
        else:
            return func(request)

    def render_template(self, template, context):
        t = self.jinja_env.get_template(template)
        return Response(t.render(**context), mimetype='text/html')

    def wsgi_app(self, environ, start_reponse):
        request = Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_reponse)

    def __call__(self, environ, start_reponse):
        if self.static_folder:
            self.wsgi_app = SharedDataMiddleware(self.wsgi_app, {self.static_url: self.static_folder})
        return self.wsgi_app(environ, start_reponse)

    def a(self, request):
        return self.render_template('a.html', {'message': json.dumps(env_to_dict(request.environ), indent=2)})

    def b(self, request):
        return self.render_template('b.html', {'message': json.dumps(env_to_dict(request.environ), indent=2)})

    def c(self, request):
        return self.render_template('c.html', {'message': json.dumps(env_to_dict(request.environ), indent=2)})

    def welcome(self, request):
        return self.render_template('index.html', {'title': json.dumps(env_to_dict(request.environ), indent=2)})

    def file_system(self, request):
        pass


if __name__ == '__main__':
    static_folder = os.path.join(os.path.abspath(__file__), 'static')
    # print(static_folder)
    app = Flask(__file__)
    run_simple(hostname='localhost', port=8013, application=app, use_reloader=True, use_debugger=True)


