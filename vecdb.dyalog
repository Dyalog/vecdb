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
    :Field Public Count←0               ⍝ Number of records
    :Field Public BlockSize←100000      ⍝ Small while we test (must be multiple of 8)
    :Field Public NumBlocks←1           ⍝ We start with one block
    :Field Public Size←0
    :Field Public noFiles←0             ⍝ in-memory database (not supported)
    :Field Public isOpen←0              ⍝ Not yet open
    :Field Public ShardFolders←⍬        ⍝ List of Shard Folders
    :Field Public ShardFn←⍬             ⍝ Shard Calculation Function
    :Field Public ShardCols←⍬           ⍝ ShardFn input column indices

    :Field _Columns←⍬
    :Field _Types←⍬

    fileprops←'Name' 'BlockSize' ⍝ To go in comp 4 of meta.vecdb
    :EndSection ⍝ Instance Fields

    :section Properties
    :property Columns
    :access public
        ∇ r←get
          r←_Columns
        ∇
    :endproperty

    :property Types
    :access public
        ∇ r←get
          r←_Types
        ∇
    :endproperty
    :endsection

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
     
      :If 0≠⍴ShardFn                      ⍝ Define shard calculation function
          findshard←⍎ShardFn
      :EndIf
     
      symbols←⎕NS¨n⍴⊂''
      :For i :In {⍵/⍳⍴⍵}'C'=⊃¨_Types        ⍝ Read symbol files for CHAR fields
          col←i⊃symbols
          col.file←folder,(⍕i),'.symbol'    ⍝ symbol file name in main folder
          col.symbol←GetSymbols col.file    ⍝ Read symbols
          col.(SymbolIndex←symbol∘⍳)        ⍝ Create lookup function
      :EndFor
     
      isOpen←1
      MakeMaps
    ∇

    ∇ MakeMaps;s;i;types;T;ns;dr;col
    ⍝ [Re]make all maps
      types←TypeNums[TypeNames⍳Types]
      symbols←⎕NS¨(≢_Columns)⍴⊂⍬
     
      :For s :In Shards
          s.count←645 1 ⎕MAP(ns.f,'counters.vecdb')'W' ⍝ Map record counter
          :For col :In s
              col.vector←(dr,¯1)⎕MAP col.file'W'
          :EndFor
     
          :If 1≠⍴sizes←∪s.(≢vector) ⍝ mapped vectors have different lengths
          :OrIf sizes∨.<s.count     ⍝ or shorter than record count
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

    ∇ make6(name folder columns types options data);i;s;offset;file;tn;type;length;col;size;n;dr;f
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
     
      'Data lengths not all the same'⎕SIGNAL(1≠≢length←∪≢¨data)/11
     
      (Name _Columns _Types)←name columns types ⍝ Update real fields
     
      symbols←⎕NS¨(≢_Columns)⍴⊂''
      :For i :In {⍵/⍳⍴⍵}'C'=⊃¨_Types        ⍝ Create symbol files for CHAR fields
          col←i⊃symbols
          col.symbol←∪i⊃data                ⍝ Unique symbols in input data
          col.file←folder,(⍕i),'.symbol'    ⍝ symbol file name in main folder
          col.symbol PutSymbols col.file    ⍝ Read symbols
          col.(SymbolIndex←symbol∘⍳)        ⍝ Create lookup function
          (i⊃data)←col.SymbolIndex i⊃data   ⍝ Convert indices
      :EndFor
     
      :If 1≠≢ShardFolders ⋄ ∘ ⋄ :EndIf ⍝ ↓↓↓ Code below only works with one shard!
     
      :For f :In ⍳≢ShardFolders
          :If ~Exists folder←f⊃ShardFolders ⋄ MkDir folder ⋄ :EndIf
     
          n←⊃length ⍝ // Should be # records in the shard
          size←BlockSize×1⌈⌈n÷BlockSize ⍝ At least one block
     
          tn←(folder,'counters.vecdb')⎕NCREATE 0
          0 ⎕NAPPEND tn 645 ⍝ Number of records
          ⎕NUNTIE tn
     
          :For i :In ⍳≢_Columns ⍝ For each column
              dr←(TypeNames⍳_Types[i])⊃TypeNums
              tn←(folder,(⍕i),'.vector')⎕NCREATE 0
              (size⍴0)⎕NAPPEND tn,dr
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
      (ShardFn ShardCols)⎕FAPPEND tn  ⍝ 7
     
      ⎕FUNTIE tn
     
      Open,⊂folder ⍝ now open it properly
    ∇

    ∇ ExtendShard(folder cols count data);i;file;tn;Type;char;tns;sym;m;ix;fp;dr
    ⍝ Extend a Shard by count items (using "data" if present)
     
      :For i :In ⍳≢cols ⍝ For each column
          dr←(TypeNames⍳⊂cols[i].type)⊃TypeNums
          tn←cols[i].file ⎕NTIE 0
          (count↑i⊃data)⎕NAPPEND tn,dr
          ⎕NUNTIE tn
      :EndFor
    ∇

    ∇ r←Close
      :Access Public
      :If ~0∊⍴Shards
          Shards.⎕EX'v' ⍝ remove all maps
      :EndIf
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
              :If (⊂name)∊'BlockSize' 'InitBlocks' 'Folders' 'Sharding'
                  ⍎name,'←options.',name
              :Else
                  ('Invalid option name: ',name)⎕SIGNAL 11
              :EndIf
          :EndFor
      :EndIf
     
    ∇

    ∇ r←Query(where cols);col;value;ix;j;s
      :Access Public
     
      :If (2=≢where)∧where[1]∊Columns ⍝ single constraint?
          where←,⊂where
      :EndIf
     
      r←2 0⍴0
     
      :For s :In ⍳≢Shards
          Data←s⊃Shards
          ix←⎕NULL
     
          :For (col value) :In where ⍝ AND them all together
              j←Columns⍳⊂col
              :If 'C'=⊃j⊃Types ⍝ Char
                  value←Data[j].SymbolIndex value
              :EndIf
              ('Invalid column name(s): ',⍕col)⎕SIGNAL((⊂col)∊Columns)↓11
              :If ⎕NULL≡ix ⋄ ix←{⍵/⍳⍴⍵}(Count↑Data[j].value)∊value
              :Else ⋄ ix/⍨←Data[j].value[ix]∊value
              :EndIf
     
              :If 0=⍴ix ⋄ :Leave ⋄ :EndIf
          :EndFor ⍝ Clause
     
          :If 0=⍴cols ⋄ r⍪←s,⍪ix ⍝ No columns; Just return indices
          :Else
              r⍪←Read ix cols
          :EndIf
      :EndFor ⍝ Shard
     
    ∇

    ∇ r←Read(indices cols);char;m;num;cix
      ⍝ Read specified indices of named columns
      :Access Public
     
      :If 1=≡cols ⋄ cols←,⊂cols ⋄ :EndIf ⍝ Single simple comlumn name
      ⎕SIGNAL/ValidateColumns cols
      r←(Data[cix←Columns⍳cols]).{v[⍵]}⊂indices
      :If 0≠⍴char←{⍵/⍳⍴⍵}'C'=⊃¨Types[cix]
          r[char]←Data[cix[char]].{s[⍵]}r[char]
      :EndIf
    ∇

    ∇ r←ValidateColumns cols;bad
     ⍝ Return result suitable for ⎕SIGNAL/
     
      r←''⍬
      :If ~0∊⍴bad←cols~Columns
          r←('Unknown Column Names:',,⍕bad)11
      :EndIf
    ∇

    ∇ r←Append(cols data);length;canupdate;shards;s;alldata;growth;tn;cix
      :Access Public
     
      'Data lengths not all the same'⎕SIGNAL(1≠≢length←∪≢¨data)/11
      'Col and Data counts not the same'⎕SIGNAL((≢cols)≠≢data)/11
      ⎕SIGNAL/ValidateColumns cols
     
      cix←Columns⍳cols
      data←Data[cix]IndexSymbols data
     
      :If 0≠canupdate←length⌊Size-Count
          (⊂Count+⍳canupdate)(Data[cix]).{
              v[⍺]←⍵
          }canupdate↑¨data
      :EndIf
     
      :If length>canupdate    ⍝ We need to extend the file
          alldata←(≢Columns)⍴⊂⍬ ⋄ alldata[cix]←canupdate↓¨data
          growth←BlockSize×(length-canupdate)(⌈÷)BlockSize ⍝ How many records to add
          shards←≢Shards
          Shards.⎕EX'v'
          :For s :In shards
              Data←s⊃Shards
              ExtendShard Folder s growth alldata'extend'
          :EndFor
     
          Size+←growth
          NumBlocks←Size÷BlockSize
          MakeMaps            ⍝ remake all the maps
      :EndIf
      Count+←length
      tn←(Folder,'meta.vecdb')⎕FSTIE 0
      (Count NumBlocks)⎕FREPLACE tn 3
      ⎕FUNTIE tn
      r←0
    ∇

    ∇ {r}←Update(indices cols data);cix
      :Access Public
     
      :If 1=≡cols ⋄ (cols data)←,∘⊂¨cols data ⋄ :EndIf ⍝ Simple col name
      ⎕SIGNAL/ValidateColumns cols
     
      cix←Columns⍳cols
      data←Data[cix]IndexSymbols data
     
      (⊂indices)(Data[cix]).{
          v[⍺]←⍵
      }data
      r←0
    ∇

    ∇ r←Delete folder;file;tn;folders;files;f
      :Access Public Shared
      ⍝ Erase a vecdb file without opening it first (it might be too damaged to open)
      ⍝ If folder does not exist, that is OK
     
      →(Exists folder)↓0 ⍝ Not there
     
      :If isWindows ⋄ folders←⎕CMD'dir "',folder,'" /B /A-D"'
      :Else ⋄ ∘ ⋄ :EndIf
      folders←AddSlash¨folders,⊂folder ⍝ process sub-folders first
     
      :For folder :In ∪folders
          :If isWindows ⋄ files←⎕CMD ⎕←'dir "',folder,'*.*" /B /A-D'
          :Else ⋄ ∘
          :EndIf
          :For file :In files
              f←folder,file
              f ⎕NERASE f ⎕NTIE 0
          :EndFor
          RmDir folder
      :EndFor
    ∇

    ∇ r←Erase;file;s;f;shards;i
      :Access Public
      ⍝ /// needs error trapping
     
      'vecdb is not open'⎕SIGNAL isOpen↓11
     
      :If Exists file←Folder,'meta.vecdb'
          shards←≢Shards
          {}Close
          :For Data :In Shards
              :For i :In ⍳≢Data.f
                  (f t)←Data[i].(f t)
                  {⍵ ⎕NERASE ⍵ ⎕NTIE 0}f,'.vector'
                  :If 'C'=⊃t ⍝ Is there a symbol file to erase?
                      {⍵ ⎕NERASE ⍵ ⎕NTIE 0}f,'.symbol'
                  :EndIf
              :EndFor
          :EndFor
          file ⎕NERASE file ⎕NTIE 0
          RmDir Folder
          r←0
      :Else
          ('Not a vector db: ',Folder)⎕SIGNAL 2
      :EndIf
    ∇

    ∇ ix←ns SymbolUpdate values;m
      ⍝ Convert values to symbol indices, and update the file if necessary
     
      :If ∨/m←(≢ns.s)<ix←ns.SymbolIndex values   ⍝ new strings found
          ns.s,←∪m/values             ⍝ Update in-memory symbol table
          ns.s PutSymbols ns.f,'.symbol' ⍝ ... update the symbol file
          ns.(SymbolIndex←s∘⍳)        ⍝ ... and the hash lookup function
          ix←ns.SymbolIndex values    ⍝ ... and use the function
      :EndIf
    ∇

    ∇ data←Data IndexSymbols data;char
    ⍝ Convert all char columns to indices
     
      :If 0≠⍴char←{⍵/⍳⍴⍵}'C'=⊃¨Data.t
          data[char]←Data[char]SymbolUpdate¨data[char]
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