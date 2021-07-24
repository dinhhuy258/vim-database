# vim-database

## Introduction

Database management for Neovim. 

- Database explorer
- Ability to execute queries
- Edit/ copy/ delete data
- ...

The supported databases at the moment:

- SQLite
- MySQL
- PostgreSQL

## Requirements

- Neovim (vim is not supported)
- Python 3.7
- sqlite3
- psql
- mysql

## Install

Install pynvim

```sh
pip3 install pynvim
```

Install Neovim plugin

```sh
Plug 'dinhhuy258/vim-database', {'branch': 'master', 'do': ':UpdateRemotePlugins'}
```

Install sqlite3 (Optional - skip if you don't use sqlite)

```sh
brew install sqlite
```

Install postgres client (Optional - skip if you don't use postgres)

```sh
brew install postgresql
```

Install msyql client (Optional - skip if you don't use mysql)

```sh
brew install mysql
```

## Usage

- `VDToggleDatabase`: Open or close database management
- `VDToggleQuery`: Open or close query terminal
- `VimDatabaseListTablesFzf`: List all tables in fzf

You can map these commands to another keys:

```VimL
nnoremap <silent> <F3> :VDToggleDatabase<CR>
nnoremap <silent> <F4> :VDToggleQuery<CR>
nmap <silent> <Leader>fd :VimDatabaseListTablesFzf<CR>
```

## Key bindings

Check the default key binding [here](https://github.com/dinhhuy258/vim-database/blob/master/rplugin/python3/database/configs/config.py)

## Configuration

You can tweak the behavior of Database by setting a few variables in your vim setting file. For example:

```VimL
let g:vim_database_rows_limit = 50
let g:vim_database_window_layout = "bottom"
...
```

### g:vim_database_rows_limit

The maximum number of rows to display in table results.

Default: `50`

### g:vim_database_window_layout

Set the layout for database window.

Possible values:
- `left`
- `right`
- `above`
- `below`

Default: `left`

### g:vim_database_window_size

Set size for database window.

Default: `100`


## Features

Navigate between connection, database, table mode:

- `<Leader> c`: Go to connection mode
- `<Leader> d`: Go to database mode
- `<Leader> t`: Go to table mode

### Connection mode

- New connection (press `c`)
- Delete connection (press `dd`)
- Select connection (press `s`)
- Modify connection (press `m`)

![](https://user-images.githubusercontent.com/17776979/126873230-3040adc1-a447-48c8-8d08-ee48c1b7f6c7.gif)

![](https://user-images.githubusercontent.com/17776979/126873229-b11b7b64-21d8-4d6b-baa0-0715fea4df6e.gif)

### Database mode

- Select database (press `s`)

![](https://user-images.githubusercontent.com/17776979/126873228-c7557467-a8c2-48bf-854e-a1b4f7bc6900.gif)

### Table mode

- Filter table (press `f`)
- Clear filter (press `F`)
- Select table (press `s`)
- Delete table (press `dd`)
- Describe table (press `.`)

![](https://user-images.githubusercontent.com/17776979/126873227-156b4675-a757-438a-be9d-445bf2e76933.gif)

### Data mode

- Sort asc (press `o`)
- Sort desc (press `O`)
- Filter (press `f`)
- Clear filter (press `F`)
- Filter columns (press `a`)
- Clear filter columns (press `A`)
- Delete row (press `dd`)
- Modify row at column (press `m`)
- Copy row (press `p`)
- Show create row query (press `C`)
- Show update row query (press `M`)
- Show update row query (press `P`)
- Describe table (press `.`)

![](https://user-images.githubusercontent.com/17776979/126873221-ecc5081e-ecf2-4ca5-be0f-2b9c1658495a.gif)

### Query mode

- Execute the query (press `r`)

![](https://user-images.githubusercontent.com/17776979/126873722-d9445e96-555b-4c5a-8eab-0f3495994c73.gif)
