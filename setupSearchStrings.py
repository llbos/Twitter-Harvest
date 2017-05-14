#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 17:00:48 2017

@author: llbos
"""
def main():
    #Define each one of your search strings.  For each element declare in the second list whether you want to backfill.  A GapFile for that searchString will negate this.
     searchstrings=['search1','search2']
     BackFill=[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]
     if len(searsearchstrings)!=len(BackFill):
         print('BackFill vector has a different number of elements than the searchstrings.  Setting all backfill to "on".')
         BackFill=[1]*len(searchstrings)
     

#     
     return searchstrings, BackFill

if __name__ == "__main__":
    main()
