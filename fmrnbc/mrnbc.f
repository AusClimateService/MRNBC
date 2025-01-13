c*********************** Main ***************************
      
      subroutine mrnbc(nycur, nsc, nyfut, nsf, ngcur, 
     1         nsgc, ngfut, nsgf, nvar, itime, 
     2         miss, iband, phlwr, phupr, igg, ilimit, thres,
     3         nday, idays, nyrmax, nvarmax, gcmc, gcmf, rec, ref,
     4         gcmcbc, gcmfbc)

        integer nycur, nsc, nyfut, nsf
        integer ngcur, nsgc, ngfut, nsgf
        integer nvar, itime, iband

        integer ndaymax, nsmax, monmax, ndmax
        integer nyrmax, nvarmax, nnmax, mvmax, nsplmax
        parameter (ndaymax=366, nsmax=6, monmax=13, ndmax=31, mvmax=31)
c        parameter (nyrmax=50, nvarmax=5, mvmax=31, nygmax=50)
c        parameter (nnmax=nyrmax*ndaymax, mmmax=nyrmax*mvmax)
        parameter (nsplmax=4)
        real*4 gcmc(nvarmax, nyrmax, monmax, ndmax)
        real*4 gcmf(nvarmax, nyrmax, monmax, ndmax)
        real*4 rec(nvarmax, nyrmax, monmax, ndmax)
        real*4 a(300)
        real*4 ref(nvarmax, nyrmax, monmax, ndmax)
        integer nday(monmax), isn, idays(monmax,4)
        integer isj(nsmax,monmax), ij(monmax), leap(4), indxx

        real*4  miss

        integer igg(nvarmax), ilimit(nvarmax)
        real*4 phlwr(nvarmax), phupr(nvarmax), thres(nvarmax)

        real*8, intent(inout) :: 
     1          gcmcbc(nyrmax * monmax * ndmax, nvarmax + 3),
     2          gcmfbc(nyrmax * monmax * ndmax, nvarmax + 3)
        integer rowidx, colidx


c       Initialize constants
        nout = 12
        leap = 0
        iseed = -1341 
        nnmax = nyrmax * ndaymax

        if(itime.eq.0) then
            if(iband.gt.15) stop 'Maximum iband value allowed is 15'
        endif

c       If threshold is greater than zero, the program corrects number
c       of occurrences below and above that threshold for that variable
        do k=1,nvar
            if(ilimit(k).le.0) thres(k)=0.0
            if(thres(k).eq.0.0) thres(k)=0.00000001
        enddo

        natm = nvar
        call day(nday, monmax)

c        print *, "Adjusting thresholds"

        call adj_thres(rec, gcmc, gcmf, nvar, nsc, nycur, nsgc, 
     1                 ngcur, ngfut, nsgf, miss, itime, nout,
     2                 leap, idays, thres, ilimit, nday, nvarmax,
     3                 nyrmax, monmax, ndmax, nnmax)

c        print *, "Done adjusting thresholds, now correcting bias"

        call standardise(nsc, nsgc, nycur, ngcur, nvar, iband, 
     1                   itime, ngfut, nsgf, leap, idays, nout,
     2                   miss, phlwr, phupr, thres, ilimit, rec, 
     4                   gcmc, gcmf, nvarmax, nyrmax, monmax, 
     5                   ndmax, nsmax, nnmax)

c        print *, "Done bias correction, now writing data to file"


c       for current climate
       rowidx = 1
       colidx = 1
        do i=1,ngcur
            ii=0
            do j=1,nout
                if(itime.ne.0) then
                    nd=1
                else
                    nd=idays(j,1)
                    if(leap(1).eq.0) then
                        nd=nday(j)
                        if(j.eq.2) call daycount(nsgc,i,nd)
                    endif
                endif
                do l=1,nd
                    if(j.eq.2.and.l.eq.29) then
                        do k=1,nvar
                            gcmc(k,i,j,l)=gcmc(k,i,j,l-1)
                        enddo
                    endif
                    ii=ii+1
                    do k=1,nvar
                        if(ilimit(k).gt.0) then
                            if(gcmc(k,i,j,l).le.0.1*thres(k)
     1                          .and.gcmc(k,i,j,l).gt.0.0)
     2                          gcmc(k,i,j,l)=0.0
                        endif
                        if(gcmc(k,i,j,l).lt.phlwr(k))
     1                      gcmc(k,i,j,l)=phlwr(k)
                        if(gcmc(k,i,j,l).gt.phupr(k))
     1                       gcmc(k,i,j,l)=phupr(k)
                        a(k)=gcmc(k,i,j,l)
c                        gcmcbc(k,i,j,l)=gcmc(k,i,j,l)
                    enddo
                    
                   gcmcbc(rowidx, 1) = i+nsgc
                   gcmcbc(rowidx, 2) = j
                   gcmcbc(rowidx, 3) = l
                   if(itime.eq.0) then
                       colidx = 4
                   else
                       colidx = 3
                   endif
                   gcmcbc(rowidx, colidx:colidx+nvar-1) = a(:natm)
                   rowidx = rowidx + 1
                enddo
            enddo
        enddo


c       for future climate
       rowidx = 1
       colidx = 1
        do i=1,ngfut
            ii=0
            do j=1,nout
                if(itime.ne.0) then
                    nd=1
                else
                    nd = idays(j,2)
                    if(leap(2).eq.0) then
                        nd=nday(j)
                        if(j.eq.2) call daycount(nsgf,i,nd)
                    endif
                endif
                do l=1,nd
                    ii=ii+1
                    if(j.eq.2.and.l.eq.29) then
                        do k=1,nvar
                            gcmf(k,i,j,l) = gcmf(k,i,j,l-1)
                        enddo
                    endif

                    do k=1,nvar
                        if(ilimit(k).gt.0) then
                            if(gcmf(k,i,j,l).le.0.1*thres(k)
     1                       .and.gcmf(k,i,j,l).gt.0.0)
     2                          gcmf(k,i,j,l)=0.0
                        endif

                        if(gcmf(k,i,j,l).lt.phlwr(k))
     1                      gcmf(k,i,j,l)=phlwr(k)
                        if(gcmf(k,i,j,l).gt.phupr(k))
     1                      gcmf(k,i,j,l)=phupr(k)
                        a(k)=gcmf(k,i,j,l)
c                        gcmfbc(k,i,j,l)=gcmf(k,i,j,l)
                    enddo
                   gcmfbc(rowidx, 1) = i+nsgf
                   gcmfbc(rowidx, 2) = j
                   gcmfbc(rowidx, 3) = l
                   if(itime.eq.0) then
                       colidx = 4
                   else
                       colidx = 3
                   endif
                   gcmfbc(rowidx, colidx:colidx+nvar-1) = a(:natm)
                   rowidx = rowidx + 1
                enddo
            enddo
        enddo


        end

c*************************************************
      subroutine check_blank(fre)
        character*200 fre,fra
        fra=' '
        do i=1,200
            if(fre(i:i).eq.' '.or.fre(i:i).eq.'	')goto 10
c           if(fre(i:i).eq.' '.or.fre(i:i).eq.'     ')goto 10
                ii=i
                goto 100
 10              continue
            enddo

 100     j=0
        do i=ii,200,1
            j=j+1
            fra(j:j)=fre(i:i)
        enddo
        fre=' '
        do i=1,200
            fre(i:i)=fra(i:i)
        enddo
        return
      end