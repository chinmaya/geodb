
# The flask Application
from flask import Flask, render_template, redirect, current_app, request
from globe_services import lookup
from globe_pipeline import init_services

app = Flask(__name__)
id_service, search_service, index_service = None, None, None
print("Initialing database")
init_services("cities1000.txt")
print("Initialization complete")

with app.app_context():
  print("Starting app {0}".format(current_app.name))
  id_service = lookup("IdLookup")
  search_service = lookup("SearchService")
  index_service = lookup("GeoIndex")

@app.route('/')
def index():
  return render_template('index.html')

@app.route('/search', methods=["GET", "POST"])
def search():
  if request.method == 'POST':
    q = request.form.get('q')
    results = search_service.search(q)
    return render_template('index.html', results=results)
  else:
    return render_template('index.html')

@app.route('/city/<int:geoid>')
def city(geoid):
  city_detail = id_service.get(geoid)
  neighbors = index_service.nearest(geoid)
  return render_template('city.html', result={"city_detail": city_detail, "neighbors": neighbors})

if __name__ == '__main__':
    app.run()
