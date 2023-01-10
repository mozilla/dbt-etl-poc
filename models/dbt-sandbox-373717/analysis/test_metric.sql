select * 
from {{ metrics.calculate(
    metric('uri_count'),
    grain='day'
) }}