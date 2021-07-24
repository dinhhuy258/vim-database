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
