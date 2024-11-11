#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

#define RC_STRATEGY_NOT_IMPLEMENTED 5

typedef struct MemoryBlock
{
    SM_PageHandle content;
    PageNumber id;
    int is_dirty;
    int pin_count;
    int lru_counter;
    int freq_counter;
} MemorySlot;

static int cache_size, disk_accesses;
static int disk_updates, hit_pos;
static int circular_counter;

void copyMemorySlot(MemorySlot *target, int pos, MemorySlot *source)
{
    target[pos].content = source->content;
    target[pos].is_dirty = source->is_dirty;
    target[pos].pin_count = source->pin_count;
    target[pos].id = source->id;
    target[pos].lru_counter = source->lru_counter;
    target[pos].freq_counter = source->freq_counter;
}

void flushMemorySlot(BM_BufferPool *const bm, MemorySlot *slot, int index)
{
    SM_FileHandle file_handle;
    openPageFile(bm->pageFile, &file_handle);
    writeBlock(slot[index].id, &file_handle, slot[index].content);
    disk_updates++;
}

void FirstInFirstOutReplacement(BM_BufferPool *const bm, MemorySlot *new_slot)
{
    int current_pos = disk_accesses % cache_size;
    MemorySlot *slots = (MemorySlot *)bm->mgmtData;

    for (int i = 0; i < cache_size; i++)
    {
        if (slots[current_pos].pin_count == 0)
        {
            if (slots[current_pos].is_dirty)
                flushMemorySlot(bm, slots, current_pos);
            copyMemorySlot(slots, current_pos, new_slot);
            return;
        }
        current_pos = (current_pos + 1) % cache_size;
    }
}

void LeastRecentlyUsedReplacement(BM_BufferPool *const bm, MemorySlot *new_slot)
{
    MemorySlot *slots = (MemorySlot *)bm->mgmtData;
    int index = -1;
    int min_counter = INT_MAX;

    for (int j = 0; j < cache_size; j++)
    {
        if (slots[j].pin_count == 0 && slots[j].lru_counter < min_counter)
        {
            min_counter = slots[j].lru_counter;
            index = j;
        }
    }

    if (index != -1)
    {
        if (slots[index].is_dirty)
            flushMemorySlot(bm, slots, index);
        copyMemorySlot(slots, index, new_slot);
    }
}

void ClockReplacement(BM_BufferPool *const bm, MemorySlot *new_slot)
{
    MemorySlot *slots = (MemorySlot *)bm->mgmtData;

    while (1)
    {
        circular_counter %= cache_size;

        if (slots[circular_counter].lru_counter != 0)
            slots[circular_counter].lru_counter = 0;
        else if (slots[circular_counter].is_dirty)
            flushMemorySlot(bm, slots, circular_counter);
        else
        {
            copyMemorySlot(slots, circular_counter, new_slot);
            circular_counter++;
            break;
        }
        circular_counter++;
    }
}

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData)
{
    MemorySlot *slots = malloc(sizeof(MemorySlot) * numPages);
    if (!slots)
        return RC_FAILED_BUFF_POOL_INIT;

    cache_size = numPages;
    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;

    for (int i = 0; i < cache_size; i++)
    {
        slots[i].id = NO_PAGE;
        slots[i].lru_counter = 0;
        slots[i].freq_counter = 0;
        slots[i].is_dirty = 0;
        slots[i].pin_count = 0;
        slots[i].content = NULL;
    }

    bm->mgmtData = slots;
    circular_counter = 0;
    disk_updates = 0;
    disk_accesses = 0;

    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    MemorySlot *slots = (MemorySlot *)bm->mgmtData;
    forceFlushPool(bm);

    for (int i = 0; i < cache_size; i++)
    {
        if (slots[i].pin_count != 0)
            return RC_PAGE_PINNED;
    }

    free(slots);
    bm->mgmtData = NULL;
    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
    MemorySlot *slots = (MemorySlot *)bm->mgmtData;

    for (int i = 0; i < cache_size; i++)
    {
        if (slots[i].is_dirty && slots[i].pin_count == 0)
        {
            flushMemorySlot(bm, slots, i);
            slots[i].is_dirty = 0;
        }
    }

    return RC_OK;
}

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    MemorySlot *slots = (MemorySlot *)bm->mgmtData;

    for (int i = 0; i < cache_size; i++)
    {
        if (slots[i].id == page->pageNum)
        {
            slots[i].is_dirty = 1;
            return RC_OK;
        }
    }

    return RC_ERROR;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    MemorySlot *slots = (MemorySlot *)bm->mgmtData;

    for (int i = 0; i < cache_size; i++)
    {
        if (slots[i].id == page->pageNum)
        {
            slots[i].pin_count--;
            return RC_OK;
        }
    }
    return RC_OK; // Consider returning an error if the page was not found
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    MemorySlot *slots = (MemorySlot *)bm->mgmtData;

    for (int i = 0; i < cache_size; i++)
    {
        if (page->pageNum == slots[i].id)
        {
            flushMemorySlot(bm, slots, i);
            slots[i].is_dirty = 0;
            return RC_OK;
        }
    }

    return RC_OK; // Consider returning an error if the page was not found
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
           const PageNumber pageNum)
{
    MemorySlot *slots = (MemorySlot *)bm->mgmtData;
    SM_FileHandle file_handle;

    for (int i = 0; i < cache_size; i++)
    {
        if (slots[i].id == pageNum)
        {
            slots[i].pin_count++;
            hit_pos++;
            slots[i].lru_counter = (bm->strategy == RS_LRU) ? hit_pos : 1;
            page->data = slots[i].content;
            page->pageNum = pageNum;
            return RC_OK;
        }
    }

    int empty_slot = -1;
    for (int i = 0; i < cache_size; i++)
    {
        if (slots[i].id == NO_PAGE)
        {
            empty_slot = i;
            break;
        }
    }

    if (empty_slot != -1)
    {
        openPageFile(bm->pageFile, &file_handle);
        slots[empty_slot].content = (SM_PageHandle)malloc(PAGE_SIZE);
        readBlock(pageNum, &file_handle, slots[empty_slot].content);
        slots[empty_slot].pin_count = 1;
        slots[empty_slot].id = pageNum;
        slots[empty_slot].freq_counter = 0;
        disk_accesses++;
        hit_pos++;
        slots[empty_slot].lru_counter = (bm->strategy == RS_LRU) ? hit_pos : 1;
        page->pageNum = pageNum;
        page->data = slots[empty_slot].content;
        return RC_OK;
    }

    // Memory allocation for new slot
    MemorySlot *new_slot = (MemorySlot *)malloc(sizeof(MemorySlot));
    if (!new_slot)
        return RC_MEM_ALLOCATION_FAIL; // Handle allocation failure

    openPageFile(bm->pageFile, &file_handle);
    new_slot->content = (SM_PageHandle)malloc(PAGE_SIZE);
    if (!new_slot->content)
    {
        free(new_slot); // Clean up if allocation fails
        return RC_MEM_ALLOCATION_FAIL;
    }

    readBlock(pageNum, &file_handle, new_slot->content);
    new_slot->id = pageNum;
    new_slot->pin_count = 1;
    new_slot->is_dirty = 0;
    new_slot->freq_counter = 0;
    hit_pos++;
    disk_accesses++;
    new_slot->lru_counter = (bm->strategy == RS_LRU) ? hit_pos : 1;

    // Call the replacement strategy
    switch (bm->strategy)
    {
    case RS_FIFO:
        FirstInFirstOutReplacement(bm, new_slot);
        break;
    case RS_LRU:
        LeastRecentlyUsedReplacement(bm, new_slot);
        break;
    case RS_CLOCK:
        ClockReplacement(bm, new_slot);
        break;
    default:
        free(new_slot->content);
        free(new_slot);
        return RC_STRATEGY_NOT_IMPLEMENTED;
    }

    page->pageNum = pageNum;
    page->data = new_slot->content;

    return RC_OK;
}

PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    PageNumber *frame_contents = (PageNumber *)malloc(sizeof(PageNumber) * cache_size);
    MemorySlot *slots = (MemorySlot *)bm->mgmtData;

    for (int i = 0; i < cache_size; i++)
        frame_contents[i] = slots[i].id;

    return frame_contents;
}

bool *getDirtyFlags(BM_BufferPool *const bm)
{
    bool *dirty_flags = malloc(sizeof(bool) * cache_size);
    MemorySlot *slots = (MemorySlot *)bm->mgmtData;

    for (int i = 0; i < cache_size; i++)
        dirty_flags[i] = slots[i].is_dirty;

    return dirty_flags;
}

int *getFixCounts(BM_BufferPool *const bm)
{
    int *fix_counts = malloc(sizeof(int) * cache_size);
    MemorySlot *slots = (MemorySlot *)bm->mgmtData;

    for (int i = 0; i < cache_size; i++)
        fix_counts[i] = slots[i].pin_count;

    return fix_counts;
}

int getNumReadIO(BM_BufferPool *const bm)
{
    return disk_accesses; // Simplified return
}

int getNumWriteIO(BM_BufferPool *const bm)
{
    return disk_updates;
}
