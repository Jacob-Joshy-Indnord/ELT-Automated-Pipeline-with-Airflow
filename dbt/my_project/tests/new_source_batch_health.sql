-- Latest batch must be non-empty and within the agreed reject-rate threshold.
with latest as (
    select * from {{ source('new_source', 'load_batches') }}
    order by loaded_at desc limit 1
)
select load_batch_id, received_count, rejected_count
from latest
where received_count = 0
   or rejected_count::float / received_count > {{ var('max_reject_rate') }}
