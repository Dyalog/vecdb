:Class vecdb
⍝ Dyalog APL vector database - see https://github.com/Dyalog/vecdb

    (⎕IO ⎕ML)←1 1

    :Section Constants
    :Field Public Shared Version←'0.2.2' ⍝ General Group By and Add/Remove Columns
    :Field Public Shared TypeNames←,¨'I1' 'I2' 'I4' 'F' 'B' 'C'
    :Field Public Shared TypeNums←83 163 323 645 11 163
    :Field Public Shared SummaryFns←'sum' 'max' 'min' 'count'
    :Field Public Shared SummaryAPLFns←'+/' '⌈/' '⌊/' '≢'
    :EndSection ⍝ Constants

    :Section Instance Fields
    :Field Public Name←''
    :Field Public Folder←''             ⍝ Where is it
    :Field Public BlockSize←100000      ⍝ Small while we test (must be multiple of 8)
    :Field Public NumBlocks←1           ⍝ We start with one block
    :Field Public noFiles←0             ⍝ in-memory database (not supported)
    :Field Public isOpen←0              ⍝ Not yet open
    :Field Public ShardFolders←⍬        ⍝ List of Shard Folders
    :Field Public ShardFn←⍬             ⍝ Shard Calculation Function
    :Field Public ShardCols←⍬           ⍝ ShardFn input column indices

    :Field _Columns←⍬
    :Field _Types←⍬
    :Field _Count←⍬

    :EndSection ⍝ Instance Fields

    fileprops←'Name' 'BlockSize' ⍝ To go in comp 4 of meta.vecdb

    :Section Properties
    :Property Columns
    :Access Public
        ∇ r←get
          r←_Columns
        ∇
    :EndProperty

    :Property Types
    :Access public
        ∇ r←get
          r←_Types
        ∇
    :EndProperty

    :Property Count
    :Access public
        ∇ r←get
          r←⊃+/_Counts.counter
        ∇
    :EndProperty

    :EndSection

    ∇ Open(folder);tn;file;props;shards;n;s;i
    ⍝ Open an existing database
     
      :Implements constructor
      :Access Public
     
      folder←AddSlash folder
     
      :Trap 0 ⋄ tn←(file←folder,'meta.vecdb')⎕FSTIE 0
      :Else ⋄ ('Unable to open ',file)⎕SIGNAL 11
      :EndTrap
      (props(_Columns _Types)ShardFolders(ShardFn ShardCols))←⎕FREAD tn(4 5 6 7)
      ⎕FUNTIE tn
     
      ⍎'(',(⍕1⊃props),')←2⊃props'
      n←≢_Columns
      s←≢ShardFolders
     
      Shards←⎕NS¨¨s⍴⊂n⍴⊂''
      Shards.name←s⍴⊂_Columns                ⍝ Column Names
      Shards.type←s⍴⊂_Types                  ⍝ Types
      Shards.file←(n/¨⊂¨ShardFolders),¨¨(,∘'.vector')¨¨s⍴⊂(⍕¨⍳n) ⍝ Vector file names
     
      :If 0≠⍴ShardFn ⋄ findshard←⍎ShardFn ⋄ :EndIf ⍝ Define shard calculation function
     
      symbols←⎕NS¨n⍴⊂''
      :For i :In {⍵/⍳⍴⍵}'C'=⊃¨_Types         ⍝ Read symbol files for CHAR fields
          col←i⊃symbols
          col.file←folder,(⍕i),'.symbol'     ⍝ symbol file name in main folder
          col.symbol←GetSymbols col.file     ⍝ Read symbols
          col.(SymbolIndex←symbol∘⍳)         ⍝ Create lookup function
      :EndFor
     
      (isOpen Folder)←1 folder
      MakeMaps
    ∇

    ∇ MakeMaps;s;i;types;T;ns;dr;col;sizes
    ⍝ [Re]make all maps
      types←TypeNums[TypeNames⍳Types]
      _Counts←⎕NS¨(≢Shards)⍴⊂⍬
     
      :For i :In ⍳≢Shards
          s←i⊃Shards
          _Counts[i].counter←645 1 ⎕MAP((i⊃ShardFolders),'counters.vecdb')'W' ⍝ Map record counter
          :For col :In ⍳≢s
              (col⊃s).vector←(types[col],¯1)⎕MAP(col⊃s).file'W'
          :EndFor
     
          :If 1≠⍴sizes←∪s.(≢vector) ⍝ mapped vectors have different lengths
          :OrIf sizes∨.<⊃(i⊃_Counts).counter ⍝ or shorter than record count
              ∘ ⍝ File damaged
          :EndIf
      :EndFor
    ∇

    ∇ make4(name folder columns types)
      :Implements constructor
      :Access Public
      make6(name folder columns types'' '') ⍝ No data or option
    ∇

    ∇ make5(name folder columns types options)
      :Implements constructor
      :Access Public
      make6(name folder columns types options'') ⍝ No data or option
    ∇

    ∇ make6(name folder columns types options data);i;s;offset;file;tn;type;length;col;size;n;dr;f;shards;d;sf
      :Implements constructor
      :Access Public
    ⍝ Create a new database
     
      folder,←((¯1↑folder)∊'/\')↓'/' ⍝ make sure we have trailing separator
      :If Exists ¯1↓folder ⍝ Folder already exists
          file←folder,'meta.vecdb'
          ('"',file,'" already exists')⎕SIGNAL(Exists file)/11
      :Else ⍝ Folder does not exist
          :Trap 0 ⋄ MkDir ¯1↓folder
          :Else ⋄ ⎕DMX.Message ⎕SIGNAL ⎕DMX.EN
          :EndTrap
      :EndIf
     
    ⍝ Validate creation parameters
      'Database must have at least one column'⎕SIGNAL(1>≢columns)⍴11
      'Column types and names do not have same length'⎕SIGNAL((≢columns)≠≢types)⍴11
      'Invalid column types - see vecdb.TypeNames'⎕SIGNAL(∧/types∊TypeNames)↓11
     
      :If 0=≢data ⋄ data←(≢columns)⍴⊂⍬ ⋄ :EndIf ⍝ Default data is all zeros
      ProcessOptions options ⍝ Sets global fields
      'Block size must be a multiple of 8'⎕SIGNAL(0≠8|BlockSize)/11
     
      ⍝ Set defaults for sharding (1 shard)
      ShardFolders,←(0=⍴ShardFolders)/⊂folder
      ShardFolders←AddSlash¨ShardFolders
      ShardCols←,ShardCols
      :If 0≠⍴ShardFn ⋄ findshard←⍎ShardFn ⋄ :EndIf ⍝ Define shard calculation function
     
      'Data lengths not all the same'⎕SIGNAL(1≠≢length←∪≢¨data)/11
     
      (Name _Columns _Types)←name columns types ⍝ Update real fields
     
      symbols←⎕NS¨(≢_Columns)⍴⊂''
      :For i :In {⍵/⍳⍴⍵}'C'=⊃¨_Types         ⍝ Create symbol files for CHAR fields
          col←i⊃symbols
          col.symbol←∪i⊃data                 ⍝ Unique symbols in input data
          col.file←folder,(⍕i),'.symbol'     ⍝ symbol file name in main folder
          col.symbol PutSymbols col.file     ⍝ Read symbols
          col.(SymbolIndex←symbol∘⍳)         ⍝ Create lookup function
          (i⊃data)←col.SymbolIndex i⊃data    ⍝ Convert indices
      :EndFor
     
      (shards data)←(⍳≢_Columns)ShardData data
      data←data,⊂⍬
     
      :For f :In ⍳≢ShardFolders
          :If ~Exists sf←f⊃ShardFolders ⋄ MkDir sf ⋄ :EndIf
     
          d←data[;shards⍳f]
          n←≢⊃d
          size←BlockSize×1⌈⌈n÷BlockSize ⍝ At least one block
     
          tn←(sf,'counters.vecdb')⎕NCREATE 0
          n ⎕NAPPEND tn 645        ⍝ Record number of records
          ⎕NUNTIE tn
     
          :For i :In ⍳≢_Columns ⍝ For each column
              dr←(TypeNames⍳_Types[i])⊃TypeNums
              tn←(sf,(⍕i),'.vector')⎕NCREATE 0
              (size↑i⊃d)⎕NAPPEND tn,dr
              ⎕NUNTIE tn
          :EndFor
      :EndFor
     
      file←folder,'meta.vecdb'
      tn←file ⎕FCREATE 0
      ('vecdb ',Version)⎕FAPPEND tn    ⍝ 1
      'See github.com/Dyalog/vecdb/doc/Implementation.md'⎕FAPPEND tn ⍝ 2
      'unused'⎕FAPPEND tn              ⍝ 3
      (fileprops(⍎¨fileprops))⎕FAPPEND tn ⍝ 4 (Name BlockSize)
      (_Columns _Types)⎕FAPPEND tn     ⍝ 5
      ShardFolders ⎕FAPPEND tn         ⍝ 6
      (ShardFn ShardCols)⎕FAPPEND tn   ⍝ 7
     
      ⎕FUNTIE tn
     
      Open,⊂folder ⍝ now open it properly
    ∇

    ∇ (shards data)←cix ShardData data;six;s;char;rawdata;sym;c
     ⍝ Shards is a vector of shards to be updated
     ⍝ data has one column per shard, and one row per item of data
     
      rawdata←data
      :If 1=≢ShardFolders ⍝ Data will necessarily all be in the 1st shard then!
          shards←,1 ⋄ data←⍪data
     
      :Else ⍝ Database *is* sharded
          'Shard Index Columns must be present in data'⎕SIGNAL((≢cix)∨.<six←cix⍳ShardCols)/11
          char←{⍵/⍳⍵}'C'=⊃¨_Types[ShardCols] ⍝ Which of the sharding cols are of type char?
     
          :If (1=≢char)∧1=≢six ⍝ There is exactly one char shard column...
          :AndIf (≢⊃sym←symbols[ShardCols].symbol)<≢⊃data ⍝ ... and fewer unique symbols than records
              s←{⍺ ⍵}⌸(findshard sym)[six⊃data]           ⍝ .. then compute shards on symbols
     
          :Else                ⍝ Not exactly one char shard column
     
              :If 0≠≢char ⍝ Are *any* char (then we must turn indices into text)
                  c←six[char] ⍝ index of character shard cols in provided data
                  data[c]←symbols[ShardCols[char]].{symbol[⍵]}data[c]
              :EndIf
     
              s←{⍺ ⍵}⌸findshard data[six]
          :EndIf
     
          shards←s[;1]
          data←↑[0.5](⊂∘⊂¨s[;2])⌷¨¨⊂rawdata
      :EndIf
    ∇

    ∇ ExtendShard(folder cols count data);i;file;tn;Type;char;tns;sym;m;ix;fp;dr;col
    ⍝ Extend a Shard by count items
     
      :For i :In ⍳≢cols ⍝ For each column
          col←i⊃cols
          dr←(TypeNames⍳⊂col.type)⊃TypeNums
          col.⎕EX'vector'
          tn←col.file ⎕NTIE 0
          (count↑i⊃data)⎕NAPPEND tn,dr
          ⎕NUNTIE tn
          col.vector←(dr,¯1)⎕MAP col.file'W'
      :EndFor
    ∇

    ∇ r←Close
      :Access Public
      ⎕EX'Shards' 'symbols' '_Counts'
      r←isOpen←0       ⍝ record the fact
    ∇

    ∇ unmake
      :Implements Destructor
      {}Close
    ∇

    ∇ ProcessOptions options;name
    ⍝ Extract optional fields from options
     
      :If 9=⎕NC'options'
          :For name :In options.⎕NL-2
              :If (⊂name)∊'BlockSize' 'InitBlocks' 'Folders' 'ShardCols' 'ShardFolders' 'ShardFn'
                  ⍎name,'←options.',name
              :Else
                  ('Invalid option name: ',name)⎕SIGNAL 11
              :EndIf
          :EndFor
      :EndIf
    ∇

    ∇ (summary colnames)←ParseSummary cols;p
    ⍝ Split column specifications into summaryfn colname
     
      :If 0=⍴cols ⋄ summary←colnames←⍬
      :Else
          :If 2>≡cols ⋄ cols←,⊂,cols ⋄ :EndIf ⍝ Enclose if simple
          p←p×(≢¨cols)≥p←cols⍳¨' ' ⍝ position of separator
          summary←(0⌈p-1)↑¨cols
          colnames←p↓¨cols
      :EndIf
    ∇

    ∇ r←Query args;where;cols;groupby;col;value;ix;j;s;count;Data;Cols;summary;m;i
      :Access Public
     
      (where cols groupby)←3↑args,(≢args)↓⍬ ⍬ ⍬
      :If 2=≢where ⋄ :AndIf where[1]∊Columns ⍝ just a single constraint?
          where←,⊂where
      :EndIf
     
      (summary cols)←ParseSummary cols
      'UNKNOWN SUMMARY FUNCTION'⎕SIGNAL(∧/summary∊SummaryFns,⊂'')↓11
     
      :If 0≠≢groupby ⍝ We are grouping
          :If 1=≡groupby ⋄ groupby←,⊂groupby ⋄ :EndIf ⍝ Enclose if simple
          m←(0≠≢¨summary)∨cols∊groupby ⍝ summary or one of the grouping cols?
          'ONLY SUMMARIZED COLUMNS MAY BE SELECTED WHEN GROUPING'⎕SIGNAL(∧/m)↓11
      :EndIf
     
      r←0 2⍴0 ⍝ (shard indices)
     
      :For s :In ⍳≢Shards
          Cols←s⊃Shards
          count←⊃(s⊃_Counts).counter
          ix←⎕NULL
     
          :For (col value) :In where ⍝ AND them all together
              j←Columns⍳⊂col
              :If 'C'=⊃j⊃Types ⍝ Char
                  value←symbols[j].SymbolIndex value
              :EndIf
              ('Invalid column name(s): ',⍕col)⎕SIGNAL((⊂col)∊Columns)↓11
              :If ⎕NULL≡ix ⋄ ix←{⍵/⍳⍴⍵}(count↑Cols[j].vector)∊value
              :Else ⋄ ix/⍨←Cols[j].vector[ix]∊value
              :EndIf
     
              :If 0=⍴ix ⋄ :Leave ⋄ :EndIf
          :EndFor ⍝ Clause
     
          r⍪←s ix
      :EndFor ⍝ Shard
     
      :If 0=≢cols ⋄ :GoTo 0 ⍝ Not asked to return anything: Just return indices
      :ElseIf 0=≢groupby    ⍝ no group by statement
          r←Read r cols
          :For i :In (0≠≢¨summary)/⍳≢cols
              (i⊃r)←⍎((SummaryFns⍳summary[i])⊃SummaryAPLFns),'i⊃r'
          :EndFor
      :Else
          r←Summarize r summary cols groupby
      :EndIf
    ∇

    ∇ r←Summarize(ix summary cols groupby);char;m;num;s;indices;fns;cix;allix;allcols;numrecs;blksize;offset;groupfn;t;multi;split;data;recs;groupix;colix;z
      ⍝ Read and Summarize specified indices of named columns
      ⍝ Very similar to Read, but not public - called by Query
     
      allix←Columns⍳allcols←groupby∪cols
      groupix←allcols⍳groupby
      colix←allcols⍳cols
     
      fns←(SummaryAPLFns,⊂'')[SummaryFns⍳summary]
     
      :If 1=≢cols ⍝ Only one summarized column
          groupfn←⍎'{(↑[0.5]⍺){⍺,',(1⊃fns),'⍵}⌸⊃⍵}'
      :Else       ⍝ More than one summarized column
          z←⊂'r←keys groupfn data'
          :If 1=≢groupix ⋄ z,←⊂'keys←⊃keys' ⋄ :Else ⋄ z,←⊂'keys←↑[0.5]keys' ⋄ :EndIf
          z,←⊂'r←',(⍕≢groupix,colix),'↑⍤1⊢keys{⍺,',(1⊃fns),'⍵}⌸⊃data'
          z,←(1↓⍳≢colix){'r[;',(⍕⍺+≢groupix),']←keys{',⍵,'⍵}⌸',(⍕⍺),'⊃data'}¨1↓fns
          :If 'groupfn'≢⎕FX z ⋄ ∘∘∘ ⋄ :EndIf
      :EndIf
     
      r←(0,≢allix)⍴0
     
      :For (s indices) :In ↓ix
          offset←0
          :If indices≡⎕NULL ⍝ All records selected
              blksize←numrecs←⊃(s⊃_Counts).counter
          :Else ⍝ <indices> records selected
              blksize←numrecs←≢indices
          :EndIf
     
          split←0     ⍝ We did it all at once
          :Repeat
              :Trap 1 ⍝ WS FULL
                  recs←blksize⌊numrecs-offset
                  :If indices≡⎕NULL ⍝ All records still selected
                      data←offset((s⊃Shards)[allix].{⍵↑⍺↓vector})recs
                  :Else
                      data←(s⊃Shards)[allix].{vector[⍵]}⊂recs↑offset↓indices
                  :EndIf
     
                  r⍪←data[groupix]groupfn data[colix]
                  offset+←blksize
                  ⎕EX'data'
              :Else ⍝ Got a WS FULL
                  split←1 ⍝ We had to go around again
                  blksize←blksize(⌈÷)2
                  ⎕←(⍕⎕AI[3]),': block size reduced: ',⍕blksize
                  :If blksize<100000
                      ∘∘∘
                  :EndIf
              :EndTrap
          :Until offset≥numrecs
     
          :If split ⍝ re-summarize partial results
              r←r[;groupix]groupfn r[;colix]
          :EndIf
     
          :For char :In {⍵/⍳⍴⍵}'C'=⊃¨Types[(≢groupby)↑allix] ⍝ Symbol Group By cols
              r[;char]←symbols[allix[char]].{symbol[⍵]}r[;char]
          :EndFor
      :EndFor
    ∇

    ∇ r←Read(ix cols);char;m;num;cix;s;indices
      ⍝ Read specified indices of named columns
      :Access Public
     
      :If 1=⍴⍴ix ⋄ ix←1,⍪⊂ix ⋄ :EndIf ⍝ Single Shard?
      :If 1=≡cols ⋄ cols←,⊂cols ⋄ :EndIf ⍝ Single simple column name
      ⎕SIGNAL/ValidateColumns cols
      cix←Columns⍳cols
      r←(⍴cix)⍴⊂⍬
     
      :For (s indices) :In ↓ix
          :If indices≡⎕NULL ⋄ r←r,¨(s⊃_Counts).counter↑¨(s⊃Shards)[cix].vector
          :Else ⋄ r←r,¨(s⊃Shards)[cix].{vector[⍵]}⊂indices ⋄ :EndIf
      :EndFor
     
      :If 0≠⍴char←{⍵/⍳⍴⍵}'C'=⊃¨Types[cix] ⍝ Symbol transation
          r[char]←symbols[cix[char]].{symbol[⍵]}r[char]
      :EndIf
    ∇

    ∇ r←ValidateColumns cols;bad
     ⍝ Return result suitable for ⎕SIGNAL/
     
      r←''⍬
      :If ~0∊⍴bad←cols~Columns
          r←('Unknown Column Names:',,⍕bad)11
      :EndIf
    ∇

    ∇ r←Append(cols data);length;canupdate;shards;s;growth;tn;cix;count;i;append;Cols;size;d;n
      :Access Public
     
      'Data lengths not all the same'⎕SIGNAL(1≠≢length←∪≢¨data)/11
      'Col and Data counts not the same'⎕SIGNAL((≢cols)≠≢data)/11
      ⎕SIGNAL/ValidateColumns cols
     
      cix←_Columns⍳cols
      data←cix IndexSymbols data ⍝ Char to Symbol indices
     
      (shards data)←(⍳≢_Columns)ShardData data
     
      :For s :In shards
          d←data[;shards⍳s]
          length←≢⊃d              ⍝ # records to be written to *this* Shard
          Cols←s⊃Shards           ⍝ Mapped columns in this Shard
          count←⊃(s⊃_Counts).counter ⍝ Active records in this Shard
          size←≢Cols[⊃cix].vector ⍝ Current Shard allocation
     
          :If 0≠canupdate←length⌊size-count ⍝ Updates to existing maps
              i←⊂count+⍳canupdate
              i(Cols[cix]).{vector[⍺]←⍵}canupdate↑¨d
          :EndIf
     
          :If length>canupdate              ⍝ We need to extend the file
              append←(≢_Columns)⍴⊂⍬
              append[cix]←canupdate↓¨d      ⍝ Data which was not updated
              growth←BlockSize×(length-canupdate)(⌈÷)BlockSize ⍝ How many records to add to the Shard
              ExtendShard(s⊃ShardFolders)Cols growth append
          :EndIf
     
          _Counts[s].counter[1]←count+length  ⍝ Update (mapped) counter
      :EndFor
     
      r←0
    ∇

    ∇ {r}←Update(ix cols data);cix;indices;s;p;i
      :Access Public
     
      :If 1=≡cols ⋄ (cols data)←,∘⊂¨cols data ⋄ :EndIf ⍝ Simple col name
      ⎕SIGNAL/ValidateColumns cols
      cix←Columns⍳cols
      'Cannot update Sharding Cols'⎕SIGNAL(cix∊ShardCols)/11
     
      data←cix IndexSymbols data
     
      :If 1=≢ix ⋄ data←⍪data ⍝ One shard
      :Else                  ⍝ Partition data by Shard
          p←(≢⊃data)⍴0 ⋄ p[+\1,≢¨¯1↓ix[;2]]←1
          data←↑p∘⊂¨data
      :EndIf
     
      :For i :In ⍳≢ix        ⍝ Each partition
          (s indices)←ix[i;]
          (⊂indices)((s⊃Shards)[cix]).{vector[⍺]←⍵}data[;i]
      :EndFor
      r←0
    ∇

    ∇ r←Delete folder;file;tn;folders;files;f
      :Access Public Shared
      ⍝ Erase a vecdb file without opening it first (it might be too damaged to open)
      ⍝   Does check whether there is a meta file in the folder
      ⍝   Also deletes
     
      folder←AddSlash folder
      'Folder not found'⎕SIGNAL(DirExists folder)↓22              ⍝ Not there
      'Not a vecdb'⎕SIGNAL(Exists file←folder,'meta.vecdb')↓22 ⍝ Paranoia
     
      :If isWindows
          ⎕CMD'rmdir "',folder,'" /s /q'
      :Else
          1 _SH'rm -r ',folder
      :EndIf
     
      r←~DirExists folder
    ∇

    ∇ r←Erase
      :Access Public
      ⍝ /// needs error trapping
     
      'vecdb is not open'⎕SIGNAL isOpen↓11
     
      {}Close
      Delete Folder
      r←0
    ∇

    ∇ ix←ns SymbolUpdate values;m
      ⍝ Convert values to symbol indices, and update the file if necessary
     
      :If ∨/m←(≢ns.symbol)<ix←ns.SymbolIndex values   ⍝ new strings found
          ns.symbol,←∪m/values             ⍝ Update in-memory symbol table
          ns.symbol PutSymbols ns.file     ⍝ ... update the symbol file
          ns.(SymbolIndex←symbol∘⍳)        ⍝ ... define new hashed lookup function
          ix←ns.SymbolIndex values         ⍝ ... and use it
      :EndIf
    ∇

    ∇ data←cix IndexSymbols data;char
    ⍝ Convert all char columns to indices
     
      :If 0≠⍴char←{⍵/⍳⍴⍵}'C'=⊃¨_Types[cix]
          data[char]←symbols[cix[char]]SymbolUpdate¨data[char]
      :EndIf
     
    ∇

    ∇ r←GetSymbols file;tn;s
    ⍝ Read and deserialise symbol table from native file
     
      tn←file ⎕NTIE 0 ⋄ s←⎕NREAD tn 83,⎕NSIZE tn ⋄ ⎕NUNTIE tn
      :Trap 0 ⋄ r←0(220⌶)s ⍝ Deseralise
      :Else ⋄ ∘ ⋄ :EndTrap ⍝ Symbol table damaged :-(
    ∇

    ∇ r←symbols PutSymbols file;tn
    ⍝ Serialise and write symbol table to native file
     
      'SYMBOL TABLE FULL'⎕SIGNAL(32767<≢symbols)/11
     
      :Trap 22
          tn←file ⎕NTIE 0 ⋄ 0 ⎕NRESIZE tn
      :Else ⋄ tn←file ⎕NCREATE 0 ⋄ :EndTrap
      (1(220⌶)symbols)⎕NAPPEND tn 83 ⍝ Serialise and append
      ⎕NUNTIE tn
    ∇

    :Section Files
    ⍝ Much of this can be lost in Dyaog 15.0 when new Cross-platform File System Functions Arrive :-)

    ∇ r←isWindows
      r←'W'=3 1⊃'.'⎕WG'APLVersion'
    ∇

    ∇ f←unixfix f
    ⍝ replaces Windows file separator \ with Unix file separator /
    ⍝ this approach is mindnumbingly simple and probably dangerous
    ⍝ which is why we call unixfix very cautiously
      :If (⊂APLVersion)∊'*nix' 'Mac' ⋄ ((f='\')/f)←'/' ⋄ :EndIf
    ∇

    ∇ r←AddSlash path
    ⍝ Ensure folder name has trailing slash
      r←path,((¯1↑path)∊'/\')↓⊃isWindows⌽'/\'
    ∇

    ∇ r←Exists path;GFA
    ⍝ Is the argument the name of an existing file or folder?
      :Select APLVersion
      :Case 'Win'
          'GFA'⎕NA'U4 kernel32.C32|GetFileAttributes* <0T '
          r←(¯1+2*32)≢GFA⊂path
      :Else
          r←1
          :Trap 22
              :Trap 19 ⍝ file access error means file exists
                  ⎕NUNTIE(unixfix path)⎕NTIE 0
              :EndTrap
          :Else
              r←0
          :EndTrap
      :EndSelect
    ∇
   
    ∇ r←DirExists path;GFA
      r←0
      :Select APLVersion
      :CaseList '*nix' 'Mac'
          :Trap 11
              r←0<⍴_SH'ls -adl ',unixfix path
          :EndTrap
      :Case 'Win'
          'GFA'⎕NA'U4 kernel32.C32|GetFileAttributes* <0T '
          r←⊃2 16⊤GFA⊂path
      :EndSelect
    ∇

    ∇ MkDir path;CreateDirectory;GetLastError;err
      ⍝ Create a folder
      :Select APLVersion
      :CaseList '*nix' 'Mac'
          :If ~DirExists path
              1 _SH'mkdir ',unixfix path
              ('mkdir error on ',path)⎕SIGNAL 11/⍨~DirExists path
          :EndIf
      :Case 'Win'
          ⎕NA'I kernel32.C32∣CreateDirectory* <0T I4' ⍝ Try for best function
          →(0≠CreateDirectory path 0)⍴0 ⍝ 0 means "default security attributes"
          ⎕NA'I4 kernel32.C32|GetLastError'
          err ⎕SIGNAL⍨'CreateDirectory error:',⍕err←GetLastError
      :EndSelect
    ∇

    ∇ {r}←{suppress}_SH cmd
    ⍝ SH cover to suppress any error messages
    ⍝ suppress will suppress error from being signaled
      :If 0=⎕NC'suppress' ⋄ suppress←0 ⋄ :EndIf
      r←''
      :Trap 0
          r←⎕SH cmd,' 2>/dev/null'
      :Else
          ('shell command failed: ',cmd)⎕SIGNAL 11/⍨~suppress
      :EndTrap
    ∇

    ∇ r←APLVersion
      :Select 3↑⊃'.'⎕WG'APLVersion'
      :CaseList 'Lin' 'AIX' 'Sol'
          r←'*nix'
      :Case 'Win'
          r←'Win'
      :Case 'Mac'
          r←'Mac'
      :Else
          ... ⍝ unknown version
      :EndSelect
    ∇
    :EndSection ⍝ Files

:EndClass