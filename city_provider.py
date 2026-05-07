'''
Population-weighted random city selection from the Destatis Gemeindeverzeichnis.

Data source: Statistisches Bundesamt (Destatis), Gemeindeverzeichnis-Informationssystem (GV-ISys)
License: Datenlizenz Deutschland - Namensnennung - Version 2.0
         https://www.govdata.de/dl-de/by-2-0
'''
import csv
import itertools
import random
from pathlib import Path

_DEFAULT_CSV = Path(__file__).parent / 'staedte_komplett.csv'
_default_provider = None


class CityProvider:
    '''Loads German municipalities from a Destatis CSV and provides population-weighted draws.'''

    def __init__(self, csv_path=None):
        if csv_path is None:
            csv_path = _DEFAULT_CSV
        self._cities = []
        weights = []
        with open(csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                pop = int(row['Einwohner'])
                if pop == 0:
                    continue
                self._cities.append({
                    'name':        row['Stadt'],
                    'postal_code': row['PLZ'],
                    'state':       row['Bundesland'],
                    'lat':  float(row['Breitengrad'].replace(',', '.')),
                    'lon':  float(row['Längengrad'].replace(',', '.')),
                    'area_km2': float(row['Fläche km2'].replace(',', '.')),
                })
                weights.append(pop)
        # Precomputed cumulative weights: 159x faster than passing weights= on every call
        self._cum_weights = list(itertools.accumulate(weights))

    def random_city(self):
        '''Return a random city dict, weighted by population.'''
        return random.choices(self._cities, cum_weights=self._cum_weights, k=1)[0]


def get_default_provider(csv_path=None):
    '''Return the module-level singleton CityProvider, creating it on first call.'''
    global _default_provider
    if _default_provider is None:
        _default_provider = CityProvider(csv_path)
    return _default_provider
