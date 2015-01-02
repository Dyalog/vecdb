:Class vecdb

    (⎕IO ⎕ML)←1 1

    :Section Constants
    :Field Public Shared Version←'0.2.0'
    :Field Public Shared TypeNames←,¨'I1' 'I2' 'I4' 'F' 'B' 'C'
    :Field Public Shared TypeNums←83 163 323 645 11 163
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
          r←⊃+/_Count
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
      _Count←(≢Shards)⍴⊂,0
     
      :For i :In ⍳≢Shards
          s←i⊃Shards
          (i⊃_Count)←645 1 ⎕MAP((i⊃ShardFolders),'counters.vecdb')'W' ⍝ Map record counter
          :For col :In ⍳≢s
              (col⊃s).vector←(types[col],¯1)⎕MAP(col⊃s).file'W'
          :EndFor
     
          :If 1≠⍴sizes←∪s.(≢vector) ⍝ mapped vectors have different lengths
          :OrIf sizes∨.<⊃i⊃_Count ⍝ or shorter than record count
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
    ⍝ If folder is empty, do not create files to back it - just keep data in memory
     
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
      :If 0≠⍴ShardFn ⋄ findshard←⍎ShardFn ⋄ :EndIf ⍝ Define shard calculation function
     
      'Data lengths not all the same'⎕SIGNAL(1≠≢length←∪≢¨data)/11
     
      (Name _Columns _Types)←name columns types ⍝ Update real fields
      (shards data)←(⍳≢_Columns)ShardData data
      data←data,⊂⍬
     
      symbols←⎕NS¨(≢_Columns)⍴⊂''
      :For i :In {⍵/⍳⍴⍵}'C'=⊃¨_Types         ⍝ Create symbol files for CHAR fields
          col←i⊃symbols
          col.symbol←∪⊃,/data[i;]            ⍝ Unique symbols in input data
          col.file←folder,(⍕i),'.symbol'     ⍝ symbol file name in main folder
          col.symbol PutSymbols col.file     ⍝ Read symbols
          col.(SymbolIndex←symbol∘⍳)         ⍝ Create lookup function
          data[i;]←col.SymbolIndex¨data[i;] ⍝ Convert indices
      :EndFor
     
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
    
    ∇ (shards data)←cix ShardData data;six;s
      :If 1=≢ShardFolders
          shards←,1 ⋄ data←⍪data
      :Else
          'Shard Index Columns must be present'⎕SIGNAL((≢cix)∨.<six←cix⍳ShardCols)/11
          s←{⍺ ⍵}⌸findshard data[six]
          shards←s[;1]
          data←↑[0.5](⊂∘⊂¨s[;2])⌷¨¨⊂data
      :EndIf
    ∇

    ∇ ExtendShard(folder cols count data);i;file;tn;Type;char;tns;sym;m;ix;fp;dr;col
    ⍝ Extend a Shard by count items (using "data" if present)
     
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
      ⎕EX'Shards' 'symbols' '_Count'
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

    ∇ r←Query(where cols);col;value;ix;j;s;count;Data;Cols
      :Access Public
     
      :If (2=≢where)∧where[1]∊Columns ⍝ single constraint?
          where←,⊂where
      :EndIf
     
      r←0 2⍴0 ⍝ (shard indices)
     
      :For s :In ⍳≢Shards
          Cols←s⊃Shards
          count←⊃s⊃_Count
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
     
      :If 0≠≢cols ⋄ r←Read r cols :EndIf
    ∇

    ∇ r←Read(ix cols);char;m;num;cix;s;indices
      ⍝ Read specified indices of named columns
      :Access Public
     
      :If 1=≡ix ⋄ ix←1,⍪⊂ix ⋄ :EndIf ⍝ Single Shard?
      :If 1=≡cols ⋄ cols←,⊂cols ⋄ :EndIf ⍝ Single simple column name
      ⎕SIGNAL/ValidateColumns cols
      cix←Columns⍳cols
      r←(⍴cix)⍴⊂⍬
     
      :For (s indices) :In ↓ix
          r←r,¨(s⊃Shards)[cix].{vector[⍵]}⊂indices
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

    ∇ r←Append(cols data);length;canupdate;shards;s;growth;tn;cix;count;i;append;Cols;size
      :Access Public
     
      'Data lengths not all the same'⎕SIGNAL(1≠≢length←∪≢¨data)/11
      'Col and Data counts not the same'⎕SIGNAL((≢cols)≠≢data)/11
      ⎕SIGNAL/ValidateColumns cols
     
      cix←_Columns⍳cols
      data←cix IndexSymbols data ⍝ Char to Symbol indices
     
      :If 1≠≢ShardFolders ⋄ ∘ ⋄ :EndIf ⍝ /// Only 1 shard at the moment
      remap←0 ⍝ re-make maps due to shard extension?
     
      :For s :In ⍳≢ShardFolders
     
          Cols←s⊃Shards
          count←⊃s⊃_Count
          size←≢Cols[⊃cix].vector
     
          :If 0≠canupdate←length⌊size-count ⍝ Updates to existing maps
              i←⊂count+⍳canupdate
              i(Cols[cix]).{vector[⍺]←⍵}canupdate↑¨data
          :EndIf
     
          :If length>canupdate              ⍝ We need to extend the file
              append←(≢_Columns)⍴⊂⍬
              append[cix]←canupdate↓¨data
              growth←BlockSize×(length-canupdate)(⌈÷)BlockSize ⍝ How many records to add
              ExtendShard(s⊃ShardFolders)Cols growth append
          :EndIf
     
          _Count[s]←count+length  ⍝ Update (mapped) counter
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
      'Folder not found'⎕SIGNAL(Exists folder)↓22         ⍝ Not there
      'Not a vecdb'⎕SIGNAL(Exists file←folder,'meta.vecdb')↓22 ⍝ Paranoia
     
      tn←0 ⋄ folders←⍬
      :Trap 0 ⋄ tn←file ⎕FTIE 0
          folders←⎕FREAD tn 6
          folders←(Exists¨folders)/folders
      :EndTrap
      ⎕FUNTIE tn∩⎕FNUMS
     
      folders←∪folders,⊂folder ⍝ process sub-folders first
     
      :For folder :In folders
          :If isWindows ⋄ files←⎕CMD'dir "',folder,'*.*" /B /A-D'
          :Else ⋄ ∘ ⍝ NIX
          :EndIf
          :For file :In files
              f←folder,file
              f ⎕NERASE f ⎕NTIE 0
          :EndFor
          RmDir folder
      :EndFor
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

    ∇ r←isWindows
      r←'W'=3 1⊃'.'⎕WG'APLVersion'
    ∇

    ∇ r←AddSlash path
    ⍝ Ensure folder name has trailing slash
      r←path,((¯1↑path)∊'/\')↓'/'
    ∇

    ∇ ok←Exists path;GFA
    ⍝ Is the argument the name of an existing file or folder?
      'GFA'⎕NA'U4 kernel32.C32|GetFileAttributes* <0T '
      ok←(¯1+2*32)≢GFA⊂path
    ∇

    ∇ MkDir path;CreateDirectory;GetLastError;err
      ⍝ Create a folder using Win32 API
     
      ⎕NA'I kernel32.C32|CreateDirectory* <0T I4' ⍝ Try for best function
      →(0≠CreateDirectory path 0)⍴0 ⍝ 0 means "default security attributes"
      ⎕NA'I4 kernel32.C32|GetLastError'
      err ⎕SIGNAL⍨'CreateDirectory error:',⍕err←GetLastError
    ∇

    ∇ RmDir path;RemoveDirectory;GetLastError
     ⍝ Remove folder using Win32 API
     
      ⎕NA'I kernel32.C32|RemoveDirectory* <0T'
      →(0≠RemoveDirectory,⊂path)⍴0
      ⎕NA'I4 kernel32.C32|GetLastError'
      11 ⎕SIGNAL⍨'RemoveDirectory error:',⍕GetLastError
    ∇
    :EndSection ⍝ Files

:EndClass