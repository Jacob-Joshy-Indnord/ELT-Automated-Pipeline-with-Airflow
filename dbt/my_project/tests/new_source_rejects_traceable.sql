-- Every reject must carry a reason and point at a known batch.
select r.reject_id
from {{ source('new_source', 'sales_rejects') }} r
left join {{ source('new_source', 'load_batches') }} b using (load_batch_id)
where b.load_batch_id is null or coalesce(r.reject_reason, '') = ''
