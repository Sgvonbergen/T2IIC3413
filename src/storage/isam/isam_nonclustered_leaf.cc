#include "isam_nonclustered_leaf.h"

#include "relational_model/system.h"

// constructor for existing page
IsamNonClusteredLeaf::IsamNonClusteredLeaf(
    const IsamNonClustered& isam,
    uint64_t page_number
) :
    isam (isam),
    page (buffer_mgr.get_page(isam.leaf_file_id, page_number))
{
    N = reinterpret_cast<uint32_t*>(page.data());

    prev = reinterpret_cast<int32_t*>(page.data() + sizeof(*N));

    next = reinterpret_cast<int32_t*>(page.data() + sizeof(*N) + sizeof(*prev));

    overflow = reinterpret_cast<int32_t*>(
        page.data() + sizeof(*N) + sizeof(*prev) + sizeof(*next)
    );

    records = reinterpret_cast<RecordInfo*>(
        page.data() + sizeof(*N) + sizeof(*prev) + sizeof(*next) + sizeof(*overflow)
    );
}


// constructor for new page
IsamNonClusteredLeaf::IsamNonClusteredLeaf(
    const IsamNonClustered& isam
) :
    isam (isam),
    page (buffer_mgr.append_page(isam.leaf_file_id))
{
    N = reinterpret_cast<uint32_t*>(page.data());

    prev = reinterpret_cast<int32_t*>(page.data() + sizeof(*N));

    next = reinterpret_cast<int32_t*>(page.data() + sizeof(*N) + sizeof(*prev));

    overflow = reinterpret_cast<int32_t*>(
        page.data() + sizeof(*N) + sizeof(*prev) + sizeof(*next)
    );

    records = reinterpret_cast<RecordInfo*>(
        page.data() + sizeof(*N) + sizeof(*prev) + sizeof(*next) + sizeof(*overflow)
    );

    *N = 0;
    *prev = -1;
    *next = -1;
    *overflow = -1;
}


IsamNonClusteredLeaf::~IsamNonClusteredLeaf() {
    assert(page.get_page_number() != static_cast<uint32_t>(*overflow));
    page.unpin();
}


void IsamNonClusteredLeaf::insert_record(RID rid, int64_t key) {
    uint32_t header_space = sizeof(*N) + sizeof(*prev) + sizeof(*next) + sizeof(*overflow);
    uint32_t space_left = page.SIZE - header_space - *N*sizeof(RecordInfo);
    if (space_left >= sizeof(RecordInfo)) {
        RecordInfo new_record(key, rid);
        records[*N] = new_record;
        *N++;
    } else {
        if (*overflow == -1) {
            IsamNonClusteredLeaf overflow_leaf(isam);
            *overflow = overflow_leaf.page.page_id.page_number;
            overflow_leaf.insert_record(rid, key);
        } else {
            IsamNonClusteredLeaf overflow_leaf(isam, *overflow);
            overflow_leaf.insert_record(rid, key);
        }
    }
}


void IsamNonClusteredLeaf::delete_record(RID rid) {
    bool found = false;
    for (uint32_t i = 0; i < *N; i++) {
        if (records[i].rid == rid) { // Found record to delete
            
            // if on last record just decreasing N will "delete", no need to move
            if (i != *N-1) {
                RecordInfo* start = records + (i + 1)*sizeof(RecordInfo);
                RecordInfo* end = records + *N*sizeof(RecordInfo);
                RecordInfo* delete_pos = records + i*sizeof(RecordInfo);
                std::move(start, end, delete_pos);
            }
            *N--;
            found = true;
        }
    }
    // if not in leaf page we must search on overflow pages
    // delete_record does not push confirmation up the stack
    // if record is not found delete fails silently
    // It is assumed that record Exists when calling this function
    if (!found) {
        IsamNonClusteredLeaf overflow_leaf(isam, *overflow);
        overflow_leaf.delete_record(rid);
    }
    
}
