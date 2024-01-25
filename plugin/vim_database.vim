if exists('s:vim_database_loaded')
   finish
endif

let s:vim_database_loaded = 1

let g:database_workspace = getcwd()

function! s:IsWinExist(winid) abort
  return !empty(getwininfo(a:winid))
endfunction

function! CloseVimDatabaseQuery(bufnr, ...) abort
  call CloseVimDatabaseQueryBorder(a:bufnr)

  let s:winids = win_findbuf(a:bufnr)
  for winid in s:winids
    call nvim_win_close(winid, v:true)
  endfor
endfunction

function! CloseVimDatabaseQueryBorder(bufnr, ...) abort
  let s:winid = getbufvar(a:bufnr, 'border_winid', -1)
  if s:winid != v:null && s:IsWinExist(s:winid)
    call nvim_win_close(s:winid, v:true)
  endif

  call setbufvar(a:bufnr, 'border_winid', -1)
endfunction

function! s:VimDatabaseSelectTable(table) abort
  call VimDatabase_select_table_fzf(a:table)
endfunction

function! VimDatabaseSelectTables(tables) abort
 call fzf#run(fzf#wrap({
        \ 'source': a:tables,
        \ 'sink': function('s:VimDatabaseSelectTable')
        \ }))
endfunction

command! VimDatabaseListTablesFzf call VimDatabase_list_tables_fzf()

if get(g:, 'huy_duong_workspace', 0) == 1
  nnoremap <silent> dbb :VDToggleDatabase<CR>
  nnoremap <silent> dbr :VDToggleQuery<CR>

  let g:vim_database_window_layout = "below"
  let g:vim_database_window_size = 25
endif

