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

if get(g:, 'huy_duong_workspace', 0) == 1
  nnoremap <silent> <F3> :VDToggleDatabase<CR>
  nnoremap <silent> <F4> :VDToggleQuery<CR>

  let g:vim_database_window_layout = "below"
  let g:vim_database_window_size = 25
endif

