import sanic

from blackkeys.blueprints.auth import blueprint as auth_blueprint
from blackkeys.blueprints.events import blueprint as events_blueprint
from blackkeys.blueprints.index import blueprint as index_blueprint

app = sanic.Sanic(__name__)
app.blueprint(index_blueprint)
app.blueprint(auth_blueprint)
app.blueprint(events_blueprint)
