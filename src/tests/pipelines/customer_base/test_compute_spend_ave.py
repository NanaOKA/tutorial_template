import pandas as pd
from tutorial_template.pipelines.customer_base.nodes import compute_spend_average


def test_compute_spend_ave():
    test_data = pd.DataFrame({'total_spend':[1,2,3]})
    assert compute_spend_average(test_data) == 2