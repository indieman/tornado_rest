import pkgutil
import os
from settings import apps_dir

url_patterns = []

walk_attrs = dict(path=[apps_dir], onerror=lambda x: None)

for _, name, _ in pkgutil.walk_packages(**walk_attrs):
    if not name.startswith('_'):
        urls = __import__('apps.{0}.urls'.format(name))
        app_url_patterns = urls.__dict__.get(name).__dict__.get('urls')\
            .url_patterns
        url_patterns.extend(app_url_patterns)
