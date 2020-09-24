# Interactive Usage

Make sure your Jupyter notebook accepts the kernel that runs the dev modules via https://stackoverflow.com/a/44786736/3991164:

```sh
# replace myenv accordingly
source activate myenv
conda install ipykernel
python -m ipykernel install --user --name myenv --display-name "Python 3.7 (myenv)"
```

In the notebook, initialize like so:

```python
# initialize interactive johanna
import johanna
johanna.interactive(dotfolder="~/.dwd-cdc", dbname="hr-temp.sqlite")

# initialize matplotlib
from matplotlib import pyplot as plt
%matplotlib inline

from dwdcdc.interactive import trend
from dwdcdc.const import MANNHEIM, POTSDAM
```


