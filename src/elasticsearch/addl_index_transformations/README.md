This directory provides code that cleans and reorganizes HuBMAP documents
before they go to the `portal` Elasticsearch index.
It relies on [`search-schema`](https://github.com/hubmapconsortium/search-schema) as a git submodule, so a couple extra steps are needed after checkout:
```
git submodule init
git submodule update
```

Git does not update submodules on pull by default...
but you can make it the default:
```
git config --global submodule.recurse true # Run this once...
git pull                                   # Now pulls submodules!
```

(I would be very happy if there were tests and linting across this whole repo, but for now this directory is a kingdom unto itself.
There is `.travis.yml` at the top level, but otherwise this is self-contained.)
