if exists('s:vim_database_loaded')
   finish
endif

let s:vim_database_loaded = 1

let g:database_workspace = getcwd()

if get(g:, 'huy_duong_workspace', 0) == 1
  nnoremap <silent> <F3> :VDShowConnections<CR>
endif

