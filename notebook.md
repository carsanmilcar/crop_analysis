```python
import pandas as pd
import requests

# Fetch the data.
df = pd.read_csv("https://ourworldindata.org/grapher/cocoa-bean-production.csv?v=1&csvType=full&useColumnShortNames=true", storage_options = {'User-Agent': 'Our World In Data data fetch/1.0'})

# Fetch the metadata
metadata = requests.get("https://ourworldindata.org/grapher/cocoa-bean-production.metadata.json?v=1&csvType=full&useColumnShortNames=true").json()
```


```python
df.columns[-1]
```




    'cocoa_beans__00000661__production__005510__tonnes'




```python
dff=df[df['Entity']=='Africa'][['Year','cocoa_beans__00000661__production__005510__tonnes']]
```


```python
dff
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Year</th>
      <th>cocoa_beans__00000661__production__005510__tonnes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1961</td>
      <td>835368.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1962</td>
      <td>867170.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1963</td>
      <td>922621.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1964</td>
      <td>1190061.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1965</td>
      <td>874245.0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>57</th>
      <td>2018</td>
      <td>3784529.8</td>
    </tr>
    <tr>
      <th>58</th>
      <td>2019</td>
      <td>3741801.8</td>
    </tr>
    <tr>
      <th>59</th>
      <td>2020</td>
      <td>3808714.0</td>
    </tr>
    <tr>
      <th>60</th>
      <td>2021</td>
      <td>4022474.0</td>
    </tr>
    <tr>
      <th>61</th>
      <td>2022</td>
      <td>4103661.0</td>
    </tr>
  </tbody>
</table>
<p>62 rows × 2 columns</p>
</div>




```python

```
