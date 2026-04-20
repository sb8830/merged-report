"""
app.py  —  Invesmate Analytics Dashboard  (Streamlit)
Single merged app — Online + Offline + Integrated dashboards
Live MS365 data OR manual file upload.
"""
import streamlit as st
import streamlit.components.v1 as components
import json, os, hashlib, secrets
from pathlib import Path
from PIL import Image
from data_processor import process_all
try:
    from ms365_connector import fetch_excel_files, check_secrets_configured
except ImportError as _ms_err:
    st.error(f"ms365_connector import error: {_ms_err}")
    st.stop()

# ─── PAGE CONFIG ──────────────────────────────────────────────────────────────
def _get_page_icon():
    for p in [Path(__file__).parent/'logo.png', Path(os.getcwd())/'logo.png']:
        if p.exists():
            try: return Image.open(p)
            except: pass
    return "📊"

st.set_page_config(page_title="Invesmate Analytics", page_icon=_get_page_icon(),
                   layout="wide", initial_sidebar_state="collapsed")
st.markdown("""<style>
  #MainMenu,footer,header{visibility:hidden}
  .block-container{padding:0!important;max-width:100%!important}
  .stApp{background:#060910}
  div[data-testid="stToolbar"]{display:none}
  section[data-testid="stSidebar"]{display:none}
  div[data-testid="stDecoration"]{display:none}
  div[data-testid="stStatusWidget"]{display:none}
  button[kind="header"]{display:none}
</style>""", unsafe_allow_html=True)

# ─── LOGO ─────────────────────────────────────────────────────────────────────
LOGO_B64 = "/9j/4AAQSkZJRgABAQAAAQABAAD/4gHYSUNDX1BST0ZJTEUAAQEAAAHIAAAAAAQwAABtbnRyUkdCIFhZWiAH4AABAAEAAAAAAABhY3NwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAA9tYAAQAAAADTLQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAlkZXNjAAAA8AAAACRyWFlaAAABFAAAABRnWFlaAAABKAAAABRiWFlaAAABPAAAABR3dHB0AAABUAAAABRyVFJDAAABZAAAAChnVFJDAAABZAAAAChiVFJDAAABZAAAAChjcHJ0AAABjAAAADxtbHVjAAAAAAAAAAEAAAAMZW5VUwAAAAgAAAAcAHMAUgBHAEJYWVogAAAAAAAAb6IAADj1AAADkFhZWiAAAAAAAABimQAAt4UAABjaWFlaIAAAAAAAACSgAAAPhAAAts9YWVogAAAAAAAA9tYAAQAAAADTLXBhcmEAAAAAAAQAAAACZmYAAPKnAAANWQAAE9AAAApbAAAAAAAAAABtbHVjAAAAAAAAAAEAAAAMZW5VUwAAACAAAAAcAEcAbwBvAGcAbABlACAASQBuAGMALgAgADIAMAAxADb/2wBDAAUDBAQEAwUEBAQFBQUGBwwIBwcHBw8LCwkMEQ8SEhEPERETFhwXExQaFRERGCEYGh0dHx8fExciJCIeJBweHx7/2wBDAQUFBQcGBw4ICA4eFBEUHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh7/wAARCAFMAUwDASIAAhEBAxEB/8QAHAABAAICAwEAAAAAAAAAAAAAAAYHAQUDBAgC/8QARRAAAgIBAgMFAwkFBgYBBQAAAAECAwQFEQYhQQcSMVFhcXKxEyIyMzQ1c4GRFCNCodEIFVJUksEkJUNi4fAWNkRTY/H/xAAbAQEAAgMBAQAAAAAAAAAAAAAABQYBBAcDAv/EADQRAAEDAwEFBgYCAgMBAAAAAAABAgMEBREhBhIxQVETMjRxgcEUImGRodEWUrHwMzVCQ//aAAwDAQACEQMRAD8A8ZAAAAAAAAAAAAAAAAGw0XRtT1jJVGnYdt831S5L2swqoiZUwqomqmvPquudk1CuEpyfgordlv8AC3YzfYoXa9l/JrxdNXN/qWhoPB/D+i1pYOnUxmvGco96X6siqi8wRaN+ZfoaE1xhj0TVTzxoXZ/xTrG0qNNnXW3s52/NS/UnGj9id8lGeqapCH+KFS3f6l2RSitorb2HDk5mLjre/Irr26SkkyKW61tSuIW49CMlu0nkhDNH7LOE8Duynizypr+K2T5kt0zS9P0yv5PAxasePlBbHRyOJtMq3VcrLmv8K2X6s11/F8m38jhRS85yf+xsR2K8VeqtXXqRktw3s7zlUlb3fN/Ayt34MhFvFGpTbcFTBekN/wCbZ1p69qsuby2t/KKX+xvx7D17tXKiGotcxORYD38mOaX0ivHreqP/AO8n/L+hla7qkWv+Lny9h6rsHWY0chhK1nNCwefixz6Pb2f1IHVxFqkXzvjJeUopnaq4rzYrayimxeicW/5mtJsTcI9W4U+21jM5zg3GscK8P6vKdmfptN1kuTm1s/1RD9Z7HOHcreWFbfhTfhs94/zJNjcW0NqN+JZDzcWmv0Zs8bXNLyPo5MYN9Jpxf8+RpPtl3o07q4T1N2G4uTuvUpHXexrXcROenZNObFL6P0ZEF1jh7WtIscM/T76duvd3X6nreucLIqVc4yT5pxe/wPi/HoyYOF9NdkXyamtzzjvU8Tt2ZufwpJRXd6d9EU8bA9KcT9l3DmrqU6KP2K9/xVclv7CqOK+y3iDRlO7Fh+340eferXztvYTFNdKeo0RcL0UlIa6GXguF+pAgfVtc6puFkJQkvFNbM+SRNwAAAAAAAAAAAAAAAAAAAAAAAAAAAHY0/Cys/JjjYdE7rZPZRgtyScC8C6txRkJ1VyoxE/n3SXL8j0Bwdwdo3DOLGGHQpXtfPukt5PzIyuukVLpxd0/ZpVNdHBpxXoVxwR2PuXyeXxFY0vH9nh/uy3NJ0rT9Kx44+BiVUQiuXcSTO3bZCqt2WTUYpc5SeySI7qvFFde9WDD5SSe3yklyXsXiyIhprheJMMRcfgrlXcXP76+hIbrqqYOdtka4peMpbI0eocUYdLcMWMr5Lq+Uf6kTzczKzLXPItc23vtvyXsXgjgbLvbNiIYkR9Uu8vQhpK1V7uhtM3XtSym18q6ov+Gvly9viaycpSl3puUm+re7/mYG5cqW301Mm7GxE9DUdI5y6qN+e6MAG8fGQADCaGAABgALl4ADAM7vzMbt+IAxniDnxsrIx5qVN9lbXh3W0bnA4ozKto5cY3xXVcpfr4EfBGVdmo6tFSWNM9cHs2Z7eClg6drWn5qSjaqrGvoT5Pf4M2T2a25NMq2LSNppmuZ2C1FWfK1LxhNt7L0fQo102HXV9K70NyKtXg5PU2fF/AWg8RUyd2MqMrb5t1aSfh1KO417Pdb4bnK11PKw9+V1a6eqPROk63iZ6UYy+Ttf8Ens2/R9TYW1wthKFkYzjL6UWt0yppUVtsf2czVwnJSepLk9nPKdFPGj5PZgvjtC7KcXPU87QO7Rkbd6VX8MvYUjqeBmablzxM2idN0Hs4yRYaWtiqm5YuvTmWGCoZO3LTqgA2z3AAAAAAAAAAAAAAAABmMZSkoxTcm9kl1ACTbSS3b6Fq9mHZhbqTr1TXYSqxt04U9Ze30Nt2S9msa1VrWu1Jze0qaJLkvVlwqMYRSilGK5JLlsV+4XVUVYoOPX9ERW3BGZZGuvU4sLFxsLFjj4tUKqoLZRituX+7OnrGs42nQ7rasua5Qi+a9X5Gt1/iJVd7GwHGUlylb4pei82RKycrJuU5SlJvdtvdslbDsi+pxPVZwuv1XzKpU1eFXdXK9Tu6pqmXqFm91jUE91Bckvy6nQfoZB0+mpY6ZiMiRERCMc5XLlVMAA2T4AAAAAAAAAAAAAAAAAAAAAAAAMrfdSi2mnyafMkGjcR247VOc5W1rZKf8AEv6ojwI24Wunr41ZK1PPmescrmLlFLOxsinJpjbRZGcGt009/wBfIj/HPBul8U4ThkVRryUv3d0Uu8nt18/iR7S9RydPtVtE21vzg+akicaPqmNqVKlXJRsS+dBvmn/Q5Redn6m0ydtDlW8se5L0lau8iouFQ8ucY8L6nwxqMsXOr3g/oWL6MkaI9dcSaFp2v6dPC1CmNkJL5susfYeb+0LgzO4U1FwnGVmHN/urdunk/U9rdc21Sbj9Hf5LbR1zZ03V0cRUAEsb4AAAAAAAAAAABlJtpJbtl1djnZ2oxr17Wql3vpUUyXh6v1NP2McB/wB6Xw1vVKmsSt70wa+m/P2F9QjCuCjGKjCK5JPwK/dbgqL2ES6rx/REXGt3EWNi68xJxhHflCKXnstvPciPEmvSvcsXCk41LlKa5OXovQcUa075SwsWW1ae1kl/E/JehHORaNmNl0YiVNSmvFEUp9TU5+Vo323SMAHRGoiJohHAAGQAAAAAAAAOBnAABjIwAAMjAAAyMAAGTAAAAAAAAABk5cXItxbldTZKM0+TT8facRg8pYmStVr0yi6YPprlRcoT/QdYq1KpRk4wyEucfP1Ry6/pGDrem2YGfVGyqxdV9H1IBRdZj3RuplKM4vdbP4k80HVa9Rx13to3wSU47+Pqjku0mzj6B/xFNndz6opLUlUqqmuFTgeau0HhHM4W1aVNkZTxZtum3bk15e0jB634s0DB4i0i3AzIRfeXzJ/4H5nmDi7QMzh3WbtPy4P5sn3J7cprzPm2XBKpm67vIXKiq0nbheKGnABKG8AAAAAACWdmXCd/FOuRrcXHEpaldPpt5Eb07Evz86nDxoOdtslGKXmz1J2e8NUcM8PU4cI/v5x710tublsRtzrfhYtO8vA0q2q7CPTivA3mBiUYWHViY1ca664pRilsuRoeLdXdaeDjSSm1tZJPwXkvU2PEWpx07DbTTummq0/iQGcp2TcptuTe7bfi2bGyNi+Jf8VOmUzz5/UpVXUqmW51XiYe++2+6AMHVkRGphCKVcgAGTAAAAAAAHh03BleIB18rNw8azu5GRXVJ9JM4XrGmJfbaNveK+7TpSWvRSbX7tdfQifel/if6lbqb2+GV0aMRcFqpLBHPCyRXrqhdv8AfOl/56j/AFD++dK/z2P/AKiku9Lzf6jvS83+pr/yCT+n5Nj+NRf3Uuz++dK/z2P/AKjP986X/nsf/UUl3peb/Ud6X+J/qP5A/wDp+R/Gov7qXctX02Xhm0fqZWq6d/nKP9RSPfl/if6mYfKzmoQc5SfJJD+QSf0T7mP41F/dfsXZLV9NT2edRv7x24TjZBWQlGUXzTT3TK64X4OyMpRytQlKurxUN+ckWHj0wophRTHuwitkvjzJyhqJ503pGbqLw1IGvpqencjIn7ypx00PsAEgRgAAAAAAAABl+JzYOTbh5UciiTUovw6NeTOHdtDwR4zwtmYrXoioqcD7a5WqioWPpWdVn4cb63s9tpLyfkRrtS4Rq4n0OXycFHNoTlVLq+Xga/QdRs07LUnJuqbSsj6ef5E+qnC2uNlclKMkmmupxi+2uWz1faRd3OU/ROUNWqYci6oeOMvHtxcmzHvg4WVycZRfRnEXJ288HKD/APkeBXsm9siKX8ymyYpahtREkjeZc4JmzMR6AAGwewANhw9pl2sazi6dRFud1ijy6LqzDlRqZUwqoiZUtP8As/8ACysnPiLMr3Ufm46f82XTdONVMrbGlGKbbfRI6ehabRpGk42n48FGFMFFbeaNPxrqCjVHBqltJ87Nn06Iq9LBJebgjU4Z+yIVC4Ve+5Xrw5Gg1jOlqGbO+T+antBeSOiZMHbaWnZTRNiYmMIVpzt5VVQADZPgAAAAAAAAAGUYMoGUKw7Ufv2H4aIkS3tQ+/YfhoiRz+4+Kf5nSbZ4SPyAANI3gDMU5NKKbb8ES7hfg7IzXHI1BOmjx7vVnvT00lQ7djTJr1FVFTM35FwR/R9JzdUvVWLVKXnLbkiyuGuFMPS4RttUbsjrJ+C9F6m6wMLGwaI041UYQXkvidltdS2UNojp/mfq78ehTbjepaj5I9G/lTG/g9ttg2292ATOMEEq5AAMmAAAAAAAAAAABjqA+ZK+DdSck8C2XNc69/LqiKnJj3TpuhdW9pQaaIe921lwpXROTXGinvDKsbkLG1LDpz8C7CyIKdV0XCSfqeVONtCv4d4hyNOti1GMt635xfgeqtMy4ZuFXkQa2mluvJ9UV12+cNrUdDjrGPXvkYn1my5ygzkFtlfR1K08nBdPUt1qqUa/c5KefwAWosYLh/s78PqzIydevhyh+7pb8+pUFcHZZGEVu5NJHq3s+0iOicJYOEklJVqVnrJ83/uRF5qOyp91OLtPTmR9xm7OLCcVN5k3Rx8ey6bSjBNtv0K3z8mWXl2ZE3u5vf2LoiVcbZnyeJDFg33rHu+fgkQ57dCy7EW1IoFqXJq7gUatkyqNMAAv5oAAAAAAAAAAAAAAyvEAq/tQe/EEfw0RMlfad/8AUK/DRFUm3sk22c/uHin+Z0q2eEj8jB3dK0zM1PIVOJTKbb5vojfcL8H5WoSV+YpU4658/Flj6Zp+Jp2MqcWpQils9vFm3QWiSow6TRv5NK4XqKm+SP5nfhDR8McI4mmpW5Sjfk+fiokl229gXLwMltgpo4G7saYQplTVy1L9+RcmAAbHA1QDPgt2tvIwDOAAAYAAAAAAAAAAAABlcjA67mFTIJJwVnOvInhTfzJ8479GSnOxaszDuxbkpV2wcWn4NMrfFulRkV3Rezi00/YWTiXRycau+D3U0mmcl21t3w9SlQxMIvTqTFDMq6c0PJvGGk2aJxHmadYtlVY+76x6GoLf/tF6Mq8zD1qqLStXydj6b9CoD0o5+3ga8vVPL2saPJL2ZaW9X400/GabhGz5Sey8Ej1SkowUYrZJbFJ/2cdJU8rO1eyPKC+SrfxLj1O9Y2Bdcn9GL29vQgLmq1Na2FOWPyQV2mzJj+qEH4jynlata994wfdWz5bI1xmcu/JybbbbbfqzB2qgp209O2NvJEKdI5XuVTAANw8wAAAAAAAAAAAAEnvyewAMoV7x3pOo6jxHFY9EpxcUlLbkbnhrg/E09Rvy9r79vB+EWSuW76LkYWyXqRkdshSZ0ztVXryJaS7zLA2FuiImNDCSSSSUUHt6gElhCJ3lAfIGJNRjKUmkl4thVwmQiZPp+PI1+tazg6TQ7Mq1KXSC+k//AHzI9xPxpTiKeNp21tvg7Oi/r+ZXmdmZObfK7JtlZOT33bIKtvTIcti1d+CxW+xPmw+bRv5X9Eg1fi7UNSzYKqboo762jF7fqWhjvvY9Um924Jt/kUXi/aa/eReeH9kpf/618EedjnkmdI57sroeu0FPHAyNsaYTX2OQAFiKuAAAAAAAAAAAAAAAZ3W+7JnwVk/K6fPHb3dT3S9GQvbdepuuEMiVGrRg5bRtTTXr0K3tRQ/FUDsJqiZQ2aZ+49Pqd/tW0j++OCs2iMd7a4/KV8t3uufL8keXGmm0/FHsu6uNtU6prlOLUvM8l8aaa9J4o1DBce6q7pd1eje6Oa2GX5XRL5l4tMuWqzoegexfTv2DgTEcoqNl/wC8l+fgbnjS/wCT0pVJ87JJP2I2mmYdWn6fRhVcq6Y9xbvwSIzx1c3lUUp8oxba9pmwRfGXdHKmUzkr9wlV287qpGgAdsRMaFeAAMgAAAAAAAAAAAAAAAMAbrYxwAM7mJSjCLnOSjFeMmRDijjOjD72Np+1t3g59I/1Nepqo6Zu89cG3S0ctU7djTJINZ1fC0uiVmVck9uUE92yt+JeK83VJuumTox+kYvx9ppNQzcnOvd2VbKyT82dYqVdd5aj5W6N/K+ZdLfZoqX5nau/CeQfN7sAEQTJy4nPKq99F5Yf2Sr8NfAo3D+11e+i8sT7JV7i+BZ9nf8A36e5VNpuEfr7HIACzIVIAAAAAAAAAAAAAAAHNh2yozKrYvZwkn/M4TPXc8KiNJInNXgqKh9sXCopaFUlOEZp795J/qine1HhHIz+LbcyiiUo21xbaj15otjQ7VdpWPNc33En+R25RhL6SUn5tHBO1dQ1b93llCzUdSsKo9OaGU+rINxfNz1ma337qSROeaK94il39ayHvvtLb9CybCx5rXOXkhG1q4YiGu8AN+oOupomFIgAAyAAAAAAAAAAAAAAADPqdDWdYwdKpdmXds+kV4v8jUcccRWaPVCnHindYt+8+nsKxzszJzb3dk2ysm/Mgrjd206rHGmXFgtlkdUoksi4b+VN1xHxVm6pOVdUnTR0iupHXze7AKpLM+Z289cqXKGCOBu5GmEAAPI9gAADlxftNfvIvHD+x1fhr4FG4/2iv3kXlhfY6fw18CzbO/8A09Cq7TcI/X2OUAFnQqIAAAAAAAAAAAAAAAMowZXiYUE54Os7+jxi3zhJo3RHeBnvg2xfSzf9USI4JtCzs7jIidSdp3ZjQwlu9iuta3eq5Lfi5ssReJXet7rV8lPx77ZY9g/EvT6HhXpliHRAB1hNUIpQADJgAAAAAAAAAAAAAB+BjkZQrrtW+8MZf9hCSa9qv3jj+4Qood08W/8A3kdFtHg2f7zAAI8kgAAAAAD7o+vr95fEvPC+xU/hr4FGUfX1+8viXnhfYqfw18CzbO8ZPT3KrtNwj9fY5QAWdCogAAAAAAAAAAAAAAAAAEv4Eb/ZchdFJbfoSUjXAif7Le9/GSX8iSnCNqv+xkx1Jyl/40MbFf8AEsHHWshNeLTX5lgJNbp+K8SD8ZQcNYcvBTgmS+w0m7Xq1eaHlWtVWa8jSAA6+hEqAAZMAAAAAAAAAAAAAMB9PaFMoVx2q/eWP7hCyadqv3lR7hCyhXXxb/M6LaPBx+QABHkkAAAAAAfdH19fvL4l5YP2Gn8NfAo2j6+v3l8S8sH7DT+GvgWbZzjJ6e5Vtpu7H6+xzAAs6FQAAAAAAAAAAAAAAAAAMKZQmfA8GtOtk142Pn57EhNPwjW69Gre3Obb/mbfc4BtDMslfI5OpNwtxGh1dJz4anptGfX9G+KsX58yPcd07WY96T5pxbOj2Iais7gTGrc+9Zjydcv9v5Eh4voVukSklvKtpr2dSQscnwd2RqrhM4Pa4xYVzehBWYMswdvTqV0AAyAAAAAAAAAAAAAH09oD6BTKFcdqv3lR7hCyZ9qj/wCZ0e4QwoV08W/zOjWnwbPIAAjyRAAAAAAPvH+vr95fEvLB+xU+4vgUdj/X1+8viXlg/Yqvw18CzbOcZPT3KttN3Y/X2OUAFnQqAAAAAAAAAAAAAAABkePJdeQZ2NNpeTn0UpfSmk/RGvVSpFC9y8kU+2plyIT/AEir5HTMevbZqC3T9Vuc9mTj1T7lt8IS8dnLofcUopRXgkkvyKG7XuJLqONsjHpscY1VwhyZwVkDq+qevmv5LRRUvbru9EO3/Zx1b5PPztInJtWR+UhHpy8f9i6c2mORiW0tcpxa2fnseV+z3VXo3F2Bm77QVijPfw2fI9WQnGyuNkG2pR70Wbl1YsFW2Zvn9jbu8WH73UrG6t1WyrlycW0/yPg3PFuG8bVZWRXzLV3l7eppjs9sqUqaVkiLnKIU2RqtcqKAAb55gAAAAAAAAAAAAAAyhW/an950e4Qwmfap950e4QwoN08W86NafBs8gADQJEAAAAAA+6Pr6/eXxLywfsVP4a+BR2P9fX7y+JeOD9ip/DXwLNs5xk9Pcq203dj9fY5gAWdCoAAAAAAAAAAAAAAAGfFm+4MxvlNTle1vGpb7+rNCuXN9CccH4jo0pWyW0rX3vXboVba2tSmoHIi6u0Q2qZm+9OiG0zsiOLh3ZM2lGuDlu/RHkjiXPnqmvZmfNve62Ulv0W/I9EdtGrLS+B8mKntbkbVQXnv47HmY55YYcRulXnoXi0xbsav6mYtxaaezXNHp/sm11a5wdjTlPe+hfJWLfqvBnl8sfsJ4i/uriN6bfZtjZq7vN8lLp4m5dqbt6dVTimps18PawrjihdvF+H+0aa7YredT3W3VdSD+HPzLRnGM4SjNbxaaa36MrrWcSWFqNtDTUU94vzT8Ca2HuaPjWleuqLoUSsiwu8h0gAdGI8AAAAAAAAAAAAAAArftU+86PcIYTPtU+9KfcIYUG6eLedHtPg2eQABoEiAAAAAAcmP9fX7y+JeOD9ip9xfAo7H+vr95fEvHB+xU/hr4Fm2c4yenuVbabux+vscwALOhUAAAAAAAAAAAAAAFtuYVcA7Om48svOqx4pvvSW/oupZFUVVVGuCSjFJL8iNcEYDUZZ1i8fm1rbp1ZtuJNUo0bRMrUL5KMaq3t6vocg2wuK1lYlPHqjdPNSZoIcpjmpSX9oPXVncQU6VTLevEj8/brJ/+/wAyrzt6xnW6lqeRnXPed03J+h1DcpYEgibGnIvUMaRRoxOQOTGusx8iu+qTjZXJSi10aOMHuep6q7OeIauI+GaMtSTuhFQuj1Utub/M5+L9OeVhrIqjvZUt3t4tdSiOx3ip8P8AEUcfInthZTUJp+EX0Z6Tg4WVKUWpQkt0VbefaK5srOGclUudJuOVOS8CrttwvE23EumvBznKtS+RtbcXtyT6o1B2qgrGVsDZmLnKfkqz2KxcKAAbh5gAAAAAAAAAyvEwZQBWnam/+a0rygQ4mPal971e4iHFBufin+Z0i1eDj8gADQJAAAAAAA+8f6+v3l8S8sH7FT7i+BRuP9fX7y+JeWB9hp9xfAs2zvGT09yrbTd2P19jmABZ0KgAAAAAAAAAAAAZ8Tsabizzc2vHgm+8+bXRdWdb8t+hNuEtLWLi/tVq2ttW6T6LovzIDaG7Mt1M52fmVMIbFPEsjjc41NePRCitJRgklsUv/aD4lVltPD+LZyh8/I2fXoi0eN9fo4d4fyM+2S78YtVRb5yk/A8rarnZGpahdm5M3O22Tk2zlNop3VEy1MmvTzLjaaVM9oqcDqgAtBPgAAGU2nuns0egOxHjJatpq0XNtX7ZjR/duT+nH+p5+O7oupZWkalTn4djhdVLdNdfQ066jbVRbi8eXma9TTtnZuqet9Uwqs/Dnj2JbtbxfVPoyvcvHsxsmdFqkpRez3W2/qSvgLijE4o0WvMoko3xW11fWL/odnibSY5+P8rVHbIguTS8V5M8Nmb063T/AA1Ro1V+xSK2kcjlRUwqEEB9TjKEnGaakns01s0zB1xkjXtRzVzkh1TC4MAA9DAAAAAAAMowZQBWfal971fhoh5MO1L74q/DRDygXLxT/M6RavBx+QABokgAAAAAAfdH19fvL4l46f8AYafcXwKOo+vr95fEvHT/ALDT7i+BZdneL/T3KttN3Y/X2OcAFoQqAAAAAAAAABkJbjxex39G023UcpVxTjWuc5+KS/qa1VUx0sSyyLhETmfbWq9URDu8K6U8zIWTav3FTTSa+k+i9hNLrK6apWWSUK4Ldt9D4xaK8eiNNSUYxWySW3/rKm7ceN/2eqXD+mXL5Wa/4icfGPpucXuddNfKz5e7+MdSw0FErnI1vqQntf4ulxHrjx8ab/YcZ92CT5SfmQUPm92CfhhbCxGM4IXGONsbUa3ggAB6n2AAAAAASLgPijL4W1qGXS3KiT2ur35SR6c4e1fC1vS6tQwbFOqxJ8nzi/I8gkw7NeNcvhXUlGUpWYFsv3te/h/3L1Ie6W74lu/H3k/JH11Gk7d5veL+4n0SOUnl4sUr0vnRXhJenqQ5qUG4tNNPZp+KZYujanh6tgV52FdG2mxbproaziLQY5feycSMY37buKWyn/5JHZnad0CpTVS6J15eZTKqkXOUTXmhCxtsfVkJ12SrnGUZJ7NNbNM+TqMb2yIjkXKcsEY5qouFAAPs+QAAAAZQBWfal981fhoh5L+1H76r8Pq14EQKBcvFP8zpFq8HH5AAGiSAAAAAAB90fX1+8viXlgfYqfw18CjaPr6/eXxLx0/7DT7i+BZdneMnp7lW2m7sfr7HOAC0IVAAAAAAZBkfluYO/pGm5Go3fJ1Ragn8+bXJI16mrjpY1klVERD7a1XrhEOPS8DI1DJVNCfjvKTXJLzJ/pmFTgY0aKVyS3cmubfVsabg4+n46poiuXNy6yfm2RLtM46xeF8KVFMo26jYn3Ib79z1f/v/AI5BfL5NeZkhh7uf9yTdDRKq4RMqpwdq3HNHDenywsScbNRujtGK/gT6vyPOeVfblZFmRfNzssk5Sk+rZy6pn5WpZ1uZmWytusk5Sk2dU3aGibSR7qarzUuNLTNgZhOPMAA3TZAAAAAAAAAAAAJd2dcb5/CufFd6VuDOX72lvde1Ho/h/WtP13ToZ2n3xthNc0vGL8n5HkI3nCHFGqcM6hHJwLmob/Pqb+bNeREXC1tqfnZo7/JH1lC2f5m6O/yendc0WjUId+O1V6XKa6+jIVmYmRhXurIrcZb8n4p+qfUkHA3G+k8UYsVRYqctL59Ett/y80STNxMfMpdWRXGcem65p+jFm2mqLW/sKhFVvT9FSqqFUVcphSsnyBvdZ4dyMRu3G3vpXNpfSivVdTSNbPuuLTXimdUobnT1zEfC5F+nMiXxuYuFQ+QOa8Qb+UPIGUYMoyZQrLtR++a/cIgTDtR+96vcIeUC5eKf5nR7V4OPyAANEkAAAAAAD7o+vr95fEvLA+xU/hr4FHY/2iv318S8sL7FT+GvgWbZzjJ6e5Vtpu7H6+xygAs5UAAZae24Bgzv5nNiY2RlWqqiqVkny5Lkvb5Es0Xhuqhq7NcbbFzUF9GL9fMhLpfaW3My9yKvRD3igdIuhptD0K/Okrbd6sfx3a5yXp/UmmJjU4lKporjCCXguvtfmfdtlWPTKyyUa64Ldt9Cou0ntVqpjbpnDs1OxruzyPFL3TldfdK2+S7rdG9P2TtFQOeu6xPUkHab2h4fDmPPCwJwyNRmmkt91X6vbqeetTz8rUs2zMzLpW3WPeUpM4cm+3JvldfZKyyb3lKT3bZxkpRUMdI3DeK8VLXTUrIG4Tj1AAN42QAAAAAAAAAAAAAAAAADnwcvJwcqGTiXTptg94yi9mi6ez3taqvVen8RtV2eEchLk/b5e1FHg1aqjiqm4enrzPCenjnTD0PZeNfTk0q6i2u2uS5Si90zXapoeDntylFVWtcpwW3P1XgzzXwhxtrnDV8XiZErKN/nU2PeLRdfB3aloWudyjMksDLe3Kb+a36Mr/w1bbJO0gVVT6for9Va3s4JvINR0HOw95KPy1ae/ehz5eqNU902mmmujWzLRrnCcFOE4zi/CSe6OlqGkYGcn8rTGM3/ABwSTX6Fotu3Lm4ZVN4cyBlodfl+xXYSW6fIkudwrbHeWJfGxLwjPk/1NNl6Xn4r3uxbIpfxJbr9UXWjv1DWIixvTPRTTdC9q6oVR2pfe9XuEOJl2prbVKX/ANpDSs3FyOqXqnU6Da/CR+QABpEgAAAAAAcmKt8mpf8AeviXlhJ/sdP4a+BR2H9qq99F56fCduNRGqEpydaSUU2/DyRYrDKyPfVyonDiVbaVFVI8fX2Pszt58vabbE0DVMhLelVRfWb25ezxN3g8LYtbUsq6VzXNxXJf1Nit2noKPOXoq9EKyyme/kRLHx777FXRXOyTeyUYtkh0zhW2xqzPs7keXzIvdv2voSjHx8fGh3KKoVxS22ikv/6cOqanp+l4zyc/Kpx60vGcv5JdSj3PbSpqV7OmTCL9zehoUzrqpy4mJj4lSrxqo1xXkub9r6mt4n4l0nh3FlkallQrf8Na5yb+JWnGnbFVGM8Xhylyk+X7RYvD2IqDVtUz9Wy5ZWoZVmRbLrN7kJT2uoqndpUqqJ+V/RYaW1KusmiEv7Qe0fU+JLJY2LKWJgLdKEXzkvVkEALJDCyFu4xMITscbY27rUwgAB6n2AAAAAAAAAAAAAAAAAAAAAAAADKbT3XJmAASbhjjjiLh+UViZs50r/pW/Oj/AD8C1eGO2PSstRq1qieHZ/jh86H9UUIDRqLdT1Grm69UNaakim7yansDS9Y0vU61PAz6MhPpGab/AE8Tv8tue2x43w8zLw7FZi5FtM14OEmiccK9ovFdORXjS1BXVppbWw7xCT2V8XzRv0Iua1KiZav3Np/aLjXHiTDcIxi3RzSW3UqwnXbPm35vElFl7Tf7OvDwIKT9FvfDs3lyuCVpW7kLUAANo2AAAAAADtaTGMtUxYy+i7op/qeutMopowqY1VVwj8nH6KS6eh5C097Z+O1/+WPxPSfF2vZ+jcDY+dhSrje1GO8o7rbYhLz2i7jGLjJE3OHtVYnmTScowi5TkoxXVkZ4h474Z0VSjlajXZauSrqfef8A4PPGvcZcSavKUc3VL5Q3a7kX3V/Ij8pOT3k235tmvDYUXCyv+x8RWlvF6/Yt3iftnyroyp0PCVEX/wBW3nL9Csta1vVNYvd2o5tt8uilLkvYjXAmaejhp0xG39knFTxxJ8iYAANo9gAAAAAAAAAAAAAAD//Z"
LOGO_SRC = "data:image/jpeg;base64," + LOGO_B64
LOGO_IMG  = f'<img src="{LOGO_SRC}" style="width:36px;height:36px;border-radius:50%;object-fit:cover;border:2px solid rgba(79,206,143,.5);flex-shrink:0">'

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def _hash(pw): return hashlib.sha256(pw.encode()).hexdigest()
_HERE = Path(__file__).resolve().parent

def _load_template(name):
    for p in [_HERE/f'template_{name}.html', Path(os.getcwd())/f'template_{name}.html']:
        if p.exists(): return p.read_text(encoding='utf-8')
    for p in Path(os.getcwd()).rglob(f'template_{name}.html'):
        return p.read_text(encoding='utf-8')
    return None

# ─── SESSION STATE ────────────────────────────────────────────────────────────
def _init():
    defaults = {
        'logged_in': False, 'username': '', 'role': '', 'user_name': '',
        'page': 'home', 'dashboards': None, 'active_dash': 'integrated',
        'refresh_counter': 0, 'pending': [],
    }
    for k, v in defaults.items():
        if k not in st.session_state: st.session_state[k] = v

    if 'users' not in st.session_state:
        st.session_state.users = {
            "admin":   {"hash":_hash("invesmate@2024"),"role":"admin","name":"Admin","suspended":False,"reset_token":"","is_main_admin":True},
            "analyst": {"hash":_hash("analyst@123"),   "role":"viewer","name":"Analyst","suspended":False,"reset_token":"","is_main_admin":False},
            "manager": {"hash":_hash("manager@123"),   "role":"viewer","name":"Manager","suspended":False,"reset_token":"","is_main_admin":False},
        }
    if 'ms365_enabled' not in st.session_state:
        try: ms_ok, _ = check_secrets_configured()
        except: ms_ok = False
        st.session_state.ms365_enabled = ms_ok

_init()

# ─── TEMPLATES ────────────────────────────────────────────────────────────────
TEMPLATES = {}
for _n in ['online', 'offline', 'integrated']:
    _t = _load_template(_n)
    if _t: TEMPLATES[_n] = _t
    else:
        st.error(f"❌ template_{_n}.html not found. Commit it to your repo.")
        st.stop()

# ─── DATA INJECTION ───────────────────────────────────────────────────────────
def _j(o): return json.dumps(o, ensure_ascii=False, default=str)

def build_data_js(data, mode):
    b    = _j(data.get('bcmb', []))
    i    = _j(data.get('insg', []))
    off  = _j(data.get('offline', []))
    # Offline student-level data (from Seminar Updated + Conversion + Leads)
    stu  = _j(data.get('students', []))
    ord_ = _j(data.get('orders', []))
    agg  = _j(data.get('offline_agg', {}))
    # Legacy / integrated
    sm   = _j(data.get('seminar', []))
    att  = _j(data.get('att_summary', {}))
    ct   = _j(data.get('ct_stats', {}))
    sr   = _j(data.get('sr_stats', {}))
    lc   = _j(data.get('loc_stats', {}))

    sb_js = "...BCMB_DATA.map(r=>({...r,course:'BCMB'}))"
    si_js = "...INSG_DATA.map(r=>({...r,course:'INSIGNIA'}))"
    so_js = "...OFFLINE_DATA.map(r=>({...r,course:'OFFLINE'}))"

    if mode == 'online':
        return (
            "const BCMB_DATA="+b+";const INSG_DATA="+i+";const OFFLINE_DATA=[];"
            "const ALL_DATA=["+sb_js+","+si_js+"];"
            "const STUDENTS=[];const ORDERS=[];const OFFLINE_AGG={};"
            "const SEMINAR_DATA=[];const ATTENDEE_SUMMARY={};const SALES_REP_STATS={};"
            "const COURSE_TYPE_STATS={};const LOCATION_STATS_ATT={};"
        )
    if mode == 'offline':
        return (
            "const BCMB_DATA=[];const INSG_DATA=[];const OFFLINE_DATA=[];const ALL_DATA=[];"
            "const STUDENTS="+stu+";const ORDERS="+ord_+";const OFFLINE_AGG="+agg+";"
            "const SEMINAR_DATA="+sm+";const ATTENDEE_SUMMARY="+att+";"
            "const SALES_REP_STATS="+sr+";const COURSE_TYPE_STATS="+ct+";"
            "const LOCATION_STATS_ATT="+lc+";"
        )
    # integrated
    return (
        "const BCMB_DATA="+b+";const INSG_DATA="+i+";const OFFLINE_DATA="+off+";"
        "const ALL_DATA=["+sb_js+","+si_js+","+so_js+"];"
        "const STUDENTS="+stu+";const ORDERS="+ord_+";const OFFLINE_AGG="+agg+";"
        "const SEMINAR_DATA="+sm+";const ATTENDEE_SUMMARY="+att+";"
        "const SALES_REP_STATS="+sr+";const COURSE_TYPE_STATS="+ct+";"
        "const LOCATION_STATS_ATT="+lc+";"
    )

def inject_data(tmpl, js): return tmpl.replace('// @@DATA@@', js, 1)
def build_all(data):
    return {n: inject_data(TEMPLATES[n], build_data_js(data, n)) for n in ['online','offline','integrated']}

# ─── SHARED CSS ───────────────────────────────────────────────────────────────
def inject_fonts():
    st.markdown('<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600&display=swap" rel="stylesheet">', unsafe_allow_html=True)

# ─── NAVBAR ───────────────────────────────────────────────────────────────────
def render_navbar(active='home'):
    inject_fonts()
    users     = st.session_state.users
    is_admin  = st.session_state.role == 'admin'
    is_main   = users.get(st.session_state.username, {}).get('is_main_admin', False)
    user_name = st.session_state.user_name
    pending_n = len(st.session_state.pending)

    if is_main:
        role_badge = '<span style="background:rgba(247,201,72,.12);border:1px solid rgba(247,201,72,.25);border-radius:8px;padding:2px 8px;font-size:9px;font-weight:700;color:#f7c948;text-transform:uppercase">Main Admin</span>'
    elif is_admin:
        role_badge = '<span style="background:rgba(180,79,231,.12);border:1px solid rgba(180,79,231,.25);border-radius:8px;padding:2px 8px;font-size:9px;font-weight:700;color:#b44fe7;text-transform:uppercase">Admin</span>'
    else:
        role_badge = '<span style="background:rgba(79,142,247,.12);border:1px solid rgba(79,142,247,.25);border-radius:8px;padding:2px 8px;font-size:9px;font-weight:700;color:#4f8ef7;text-transform:uppercase">Viewer</span>'

    pending_badge = (f'<span style="background:#f76f4f;color:#fff;border-radius:50%;width:16px;height:16px;display:inline-flex;align-items:center;justify-content:center;font-size:9px;font-weight:800;margin-left:4px">{pending_n}</span>' if pending_n > 0 else '')

    st.markdown(f"""
<style>
.im-nav{{background:linear-gradient(180deg,#0d1119 0%,#080b12 100%);border-bottom:1px solid rgba(255,255,255,.07);
  padding:0 24px;height:60px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:9999}}
.im-brand{{font-family:'Syne',sans-serif;font-size:16px;font-weight:800;color:#eceef5;letter-spacing:-.3px;line-height:1.1}}
.im-brand-sub{{font-size:9px;color:#4a5068;text-transform:uppercase;letter-spacing:.9px}}
.im-user-pill{{background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08);border-radius:20px;
  padding:4px 12px 4px 8px;display:flex;align-items:center;gap:7px;font-size:12px;color:#8a90aa}}
.im-dot{{width:7px;height:7px;background:#4fce8f;border-radius:50%;animation:imdot 2s infinite;flex-shrink:0}}
@keyframes imdot{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
</style>
<div class="im-nav">
  <div style="display:flex;align-items:center;gap:11px">
    {LOGO_IMG}
    <div><div class="im-brand">Invesmate</div><div class="im-brand-sub">Analytics Hub</div></div>
  </div>
  <div style="display:flex;align-items:center;gap:10px">
    <div class="im-user-pill"><div class="im-dot"></div><span>{user_name}</span></div>
    {role_badge}
  </div>
</div>""", unsafe_allow_html=True)

    if is_admin:
        cols = st.columns([2,1,1,1,1,2])
        btn_map = [(1,'🏠 Home','home'),(2,'📊 Dashboard','dashboard'),(3,f'⚙️ Admin{pending_badge}','admin'),(4,'🚪 Logout','logout')]
    else:
        cols = st.columns([2,1,1,1,2])
        btn_map = [(1,'🏠 Home','home'),(2,'📊 Dashboard','dashboard'),(3,'🚪 Logout','logout')]

    for ci, label, action in btn_map:
        with cols[ci]:
            if st.button(label, key=f'nb_{action}', use_container_width=True,
                         type="primary" if active==action else "secondary"):
                if action == 'logout':
                    _u = st.session_state.get('users',{}); _p = st.session_state.get('pending',[])
                    for k in list(st.session_state.keys()): del st.session_state[k]
                    st.session_state.users = _u; st.session_state.pending = _p
                else:
                    st.session_state.page = action
                st.rerun()

# ─── LOGIN ────────────────────────────────────────────────────────────────────
def show_login():
    inject_fonts()
    st.markdown(f"""
<style>body,.stApp{{background:#060910}}
.lshell{{min-height:100vh;display:flex;align-items:center;justify-content:center;
  background:radial-gradient(ellipse at 25% 25%,rgba(79,142,247,.1) 0%,transparent 55%),
             radial-gradient(ellipse at 75% 75%,rgba(79,206,143,.07) 0%,transparent 55%),#060910;padding:40px 20px}}
.lcard{{background:linear-gradient(145deg,#0c1018,#090d14);border:1px solid rgba(255,255,255,.08);
  border-radius:22px;padding:40px 46px;width:100%;max-width:400px;box-shadow:0 32px 100px rgba(0,0,0,.7)}}
.lt{{font-family:'Syne',sans-serif;font-size:24px;font-weight:800;color:#eceef5;text-align:center;margin:14px 0 4px;letter-spacing:-.5px}}
.ls{{font-size:11px;color:#4a5068;text-align:center;margin-bottom:30px;text-transform:uppercase;letter-spacing:.8px}}</style>
<div class="lshell"><div class="lcard">
  <div style="text-align:center"><img src="{LOGO_SRC}" style="width:76px;height:76px;border-radius:50%;object-fit:cover;border:3px solid rgba(79,206,143,.4)"></div>
  <div class="lt">Invesmate Analytics</div><div class="ls">Sign in to continue</div>
</div></div>""", unsafe_allow_html=True)

    c1,c2,c3 = st.columns([1,2,1])
    with c2:
        st.markdown("<div style='margin-top:-300px'>", unsafe_allow_html=True)
        username = st.text_input("", placeholder="👤  Username", key="lu")
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        password = st.text_input("", placeholder="🔑  Password", type="password", key="lp")
        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
        if st.button("Sign In  →", use_container_width=True, type="primary", key="lbtn"):
            u = st.session_state.users.get((username or '').strip().lower())
            if u and u['hash'] == _hash(password or ''):
                if u.get('suspended', False):
                    st.error("🚫 Your account has been suspended.")
                else:
                    st.session_state.logged_in = True
                    st.session_state.username  = username.strip().lower()
                    st.session_state.role      = u['role']
                    st.session_state.user_name = u['name']
                    st.session_state.page      = 'home'
                    st.rerun()
            else:
                st.error("❌ Invalid credentials.")
        st.markdown("</div>", unsafe_allow_html=True)

# ─── HOME ─────────────────────────────────────────────────────────────────────
def show_home():
    render_navbar('home')
    inject_fonts()
    ms_on = st.session_state.ms365_enabled

    st.markdown(f"""
<style>
.hh{{text-align:center;padding:48px 20px 32px}}
.hh1{{font-family:'Syne',sans-serif;font-size:38px;font-weight:800;color:#eceef5;margin:14px 0 8px;letter-spacing:-1px}}
.hsub{{color:#4a5068;font-size:12px;text-transform:uppercase;letter-spacing:.8px}}
.dprow{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;max-width:780px;margin:28px auto 0}}
.dp{{border-radius:12px;padding:14px 18px;font-size:12px;font-weight:700;color:#fff;text-align:center;border:1px solid}}
.dpo{{background:linear-gradient(135deg,rgba(79,142,247,.2),rgba(180,79,231,.1));border-color:rgba(79,142,247,.3)}}
.dpf{{background:linear-gradient(135deg,rgba(247,111,79,.2),rgba(180,79,231,.1));border-color:rgba(247,111,79,.3)}}
.dpi{{background:linear-gradient(135deg,rgba(79,206,143,.15),rgba(79,142,247,.1));border-color:rgba(79,206,143,.25)}}
.ibox{{background:rgba(79,142,247,.05);border:1px solid rgba(79,142,247,.12);border-radius:14px;padding:16px 20px;
  margin:20px auto;max-width:900px;color:#8a90aa;font-size:13px;line-height:1.8}}
.ibox strong{{color:#eceef5}}
.live-badge{{display:inline-flex;align-items:center;gap:5px;background:rgba(79,206,143,.1);border:1px solid rgba(79,206,143,.25);
  border-radius:20px;padding:4px 12px;font-size:11px;color:#4fce8f;font-weight:600;margin:10px auto;width:fit-content}}
.live-dot{{width:6px;height:6px;background:#4fce8f;border-radius:50%;animation:ldot 2s infinite}}
@keyframes ldot{{0%,100%{{opacity:1}}50%{{opacity:.2}}}}
@media(max-width:700px){{.dprow{{grid-template-columns:1fr}}}}</style>
<div class="hh">
  <img src="{LOGO_SRC}" style="width:88px;height:88px;border-radius:50%;object-fit:cover;border:3px solid rgba(79,206,143,.4);box-shadow:0 0 40px rgba(79,206,143,.18)">
  <div class="hh1">Invesmate Analytics Hub</div>
  <div class="hsub">{'Live Microsoft 365 data' if ms_on else 'Upload your Excel files'} · 3 interactive dashboards</div>
</div>
<div class="dprow">
  <div class="dp dpo">🎥 Online Dashboard<br><small style="font-weight:400;opacity:.8">BCMB + INSIGNIA webinars</small></div>
  <div class="dp dpf">🏢 Offline Dashboard<br><small style="font-weight:400;opacity:.8">Seminar · Students · Sales</small></div>
  <div class="dp dpi">📊 Integrated Dashboard<br><small style="font-weight:400;opacity:.8">Everything combined</small></div>
</div>""", unsafe_allow_html=True)

    mc1, mc2, mc3 = st.columns([2,2,2])
    with mc2:
        c1, c2 = st.columns(2)
        with c1:
            if st.button("☁️ Live Data"+" ●"*ms_on, key="mode_live", use_container_width=True,
                         type="primary" if ms_on else "secondary"):
                st.session_state.ms365_enabled = True; st.rerun()
        with c2:
            if st.button("📁 Upload Files"+" ●"*(not ms_on), key="mode_upload", use_container_width=True,
                         type="primary" if not ms_on else "secondary"):
                st.session_state.ms365_enabled = False; st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    if ms_on:
        st.markdown("""<div style="text-align:center"><div class="live-badge"><div class="live-dot"></div>Connected to Microsoft 365</div></div>""", unsafe_allow_html=True)
        st.markdown("""<div class="ibox">
  <strong>Live mode — 4 files fetched automatically:</strong><br>
  🔵 <strong>Free Class Lead Report</strong> — BCMB &amp; INSIGNIA webinar data<br>
  🟢 <strong>Seminar Updated Sheet</strong> — Attendance, seat bookings, student info<br>
  🟡 <strong>Conversion List</strong> — Orders, payments, course purchases<br>
  🔴 <strong>Leads Report</strong> — Lead source, campaign, stage, owner<br><br>
  Click <strong>🔄 Refresh &amp; Build</strong> to load latest data from SharePoint.
</div>""", unsafe_allow_html=True)

        if st.session_state.get('last_refresh'):
            st.markdown(f'<div style="text-align:center;font-size:11px;color:#4a5068;margin-bottom:10px">Last refreshed: {st.session_state.last_refresh}</div>', unsafe_allow_html=True)

        _, cb, _ = st.columns([1,2,1])
        with cb:
            if st.button("🔄  Refresh & Build Dashboards", use_container_width=True, type="primary", key="live_refresh"):
                with st.spinner("Fetching files from Microsoft 365…"):
                    try:
                        st.session_state.refresh_counter += 1
                        files = fetch_excel_files(st.session_state.refresh_counter)
                        with st.spinner("Parsing & building dashboards…"):
                            data = process_all(
                                webinar_file=files.get('webinar'),
                                seminar_updated_file=files.get('seminar_updated'),
                                conversion_file=files.get('conversion'),
                                leads_file=files.get('leads'),
                                sem_name='seminar_updated.xlsx',
                                conv_name='conversion.xlsx',
                                leads_name='leads.xlsx',
                            )
                        if data['errors']:
                            for e in data['errors']: st.warning(f"⚠️ {e}")
                        st.session_state.dashboards  = build_all(data)
                        st.session_state.active_dash = 'integrated'
                        from datetime import datetime
                        st.session_state.last_refresh = datetime.now().strftime("%d %b %Y, %H:%M:%S")
                        s = data['stats']
                        st.success(f"✅ Done — BCMB:{s['bcmb_count']} · INSIGNIA:{s['insg_count']} · Students:{s['students']:,} · Conversions:{s['conversions']}")
                        st.session_state.page = 'dashboard'; st.rerun()
                    except (ConnectionError, PermissionError, FileNotFoundError, ValueError) as e:
                        st.error(str(e))
                    except Exception as e:
                        st.error(f"❌ Unexpected error: {e}")
                        import traceback; st.code(traceback.format_exc())

            if st.session_state.role == 'admin':
                with st.expander("⚙️ Microsoft 365 Configuration", expanded=False):
                    _show_ms365_setup()
    else:
        st.markdown("""<div class="ibox">
  <strong>Manual upload — 4 files (1 required + 3 for offline):</strong><br>
  🔵 <strong>Free_Class_Lead_Report.xlsx</strong> — BCMB &amp; INSIGNIA sheets <em>(online dashboard)</em><br>
  🟢 <strong>Seminar_Updated_Sheet.xlsx</strong> — Attendance, seat bookings, student names<br>
  🟡 <strong>Conversion_List.xlsx</strong> — Orders, payment_received, due, course names<br>
  🔴 <strong>Leads_Report.xlsx</strong> — Lead source, campaign, stage, owner, attempted
</div>""", unsafe_allow_html=True)

        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div style="background:#0c1018;border:1px solid rgba(255,255,255,.07);border-radius:12px;padding:14px;margin-bottom:8px"><span style="font-size:20px">🔵</span><div style="font-family:Syne,sans-serif;font-size:11px;font-weight:700;color:#eceef5;margin:5px 0 2px">Free Class Lead Report</div><div style="font-size:10px;color:#4a5068">BCMB &amp; INSIGNIA online webinars</div></div>', unsafe_allow_html=True)
            wf = st.file_uploader("wf", type=['xlsx','xls','csv'], key='wf', label_visibility='collapsed')
        with c2:
            st.markdown('<div style="background:#0c1018;border:1px solid rgba(255,255,255,.07);border-radius:12px;padding:14px;margin-bottom:8px"><span style="font-size:20px">🟢</span><div style="font-family:Syne,sans-serif;font-size:11px;font-weight:700;color:#eceef5;margin:5px 0 2px">Seminar Updated Sheet</div><div style="font-size:10px;color:#4a5068">Student attendance &amp; seat bookings</div></div>', unsafe_allow_html=True)
            suf = st.file_uploader("suf", type=['xlsx','xls','csv'], key='suf', label_visibility='collapsed')

        c3, c4 = st.columns(2)
        with c3:
            st.markdown('<div style="background:#0c1018;border:1px solid rgba(255,255,255,.07);border-radius:12px;padding:14px;margin-bottom:8px"><span style="font-size:20px">🟡</span><div style="font-family:Syne,sans-serif;font-size:11px;font-weight:700;color:#eceef5;margin:5px 0 2px">Conversion List</div><div style="font-size:10px;color:#4a5068">Orders, payments, courses</div></div>', unsafe_allow_html=True)
            cvf = st.file_uploader("cvf", type=['xlsx','xls','csv'], key='cvf', label_visibility='collapsed')
        with c4:
            st.markdown('<div style="background:#0c1018;border:1px solid rgba(255,255,255,.07);border-radius:12px;padding:14px;margin-bottom:8px"><span style="font-size:20px">🔴</span><div style="font-family:Syne,sans-serif;font-size:11px;font-weight:700;color:#eceef5;margin:5px 0 2px">Leads Report</div><div style="font-size:10px;color:#4a5068">Lead source, campaign, stage, owner</div></div>', unsafe_allow_html=True)
            ldf = st.file_uploader("ldf", type=['xlsx','xls','csv'], key='ldf', label_visibility='collapsed')

        st.markdown("<br>", unsafe_allow_html=True)
        _, cb, _ = st.columns([1,2,1])
        with cb:
            ready_offline = suf and cvf  # ldf optional
            ready_any = wf or ready_offline
            if ready_any:
                if st.button("🚀  Generate All 3 Dashboards", use_container_width=True, type="primary"):
                    with st.spinner("Parsing files and building dashboards…"):
                        try:
                            data = process_all(
                                webinar_file=wf,
                                seminar_updated_file=suf,
                                conversion_file=cvf,
                                leads_file=ldf,
                                sem_name=suf.name if suf else '',
                                conv_name=cvf.name if cvf else '',
                                leads_name=ldf.name if ldf else '',
                            )
                            if data['errors']:
                                for e in data['errors']: st.warning(f"⚠️ {e}")
                            st.session_state.dashboards  = build_all(data)
                            st.session_state.active_dash = 'integrated'
                            s = data['stats']
                            st.success(f"✅ Done — BCMB:{s['bcmb_count']} · INSIGNIA:{s['insg_count']} · Students:{s['students']:,} · Conversions:{s['conversions']}")
                            st.session_state.page = 'dashboard'; st.rerun()
                        except Exception as e:
                            st.error(f"❌ {e}")
                            import traceback; st.code(traceback.format_exc())
            else:
                st.markdown('<div style="text-align:center;padding:14px;background:rgba(255,255,255,.02);border:1px solid rgba(255,255,255,.05);border-radius:10px;color:#4a5068;font-size:13px">Upload at least <strong style="color:#8a90aa">Seminar Updated Sheet + Conversion List</strong> for offline dashboard</div>', unsafe_allow_html=True)


def _show_ms365_setup():
    try:
        from ms365_connector import check_share_urls_configured
        share_status = check_share_urls_configured()
    except Exception:
        share_status = {}
    ok, missing  = check_secrets_configured()
    st.markdown("""
**Add to Streamlit Cloud → App Settings → Secrets:**
```toml
MS_EMAIL    = "admin@admininvesmate360.onmicrosoft.com"
MS_PASSWORD = "your-password"

SHARE_URL_WEBINAR        = "https://..."
SHARE_URL_SEMINAR_UPDATE = "https://..."   # Seminar Updated Sheet
SHARE_URL_CONVERSION     = "https://..."   # Conversion List
SHARE_URL_LEADS          = "https://..."   # Leads Report
```
""")
    if ok: st.success("✅ MS_EMAIL + MS_PASSWORD configured.")
    else:  st.error(f"❌ Missing: {', '.join(missing)}")
    st.markdown("**Share URL status:**")
    cols = st.columns(2)
    for i, (secret, configured) in enumerate(share_status.items()):
        with cols[i % 2]:
            st.markdown(f"`{'✅' if configured else '❌'} {secret}`")

# ─── DASHBOARD ────────────────────────────────────────────────────────────────
def show_dashboard():
    render_navbar('dashboard')
    if not st.session_state.dashboards:
        st.markdown("<div style='padding:40px;text-align:center;color:#4a5068'>No dashboards yet. Go to Home to upload files.</div>", unsafe_allow_html=True)
        _, cb, _ = st.columns([1,2,1])
        with cb:
            if st.button("← Go Home", use_container_width=True):
                st.session_state.page = 'home'; st.rerun()
        return

    active = st.session_state.active_dash
    DASH = {'online':'🎥 Online','offline':'🏢 Offline','integrated':'📊 Integrated'}

    st.markdown("<div style='background:#0a0e16;border-bottom:1px solid rgba(255,255,255,.06);padding:8px 22px'></div>", unsafe_allow_html=True)
    tc = st.columns([1,1,1,4,1])
    for idx, (key, label) in enumerate(DASH.items()):
        with tc[idx]:
            if st.button(label, key=f'dt_{key}', use_container_width=True,
                         type="primary" if key==active else "secondary"):
                st.session_state.active_dash = key; st.rerun()
    with tc[4]:
        if st.button("← New Files", use_container_width=True):
            st.session_state.dashboards = None; st.session_state.active_dash = 'integrated'
            st.session_state.page = 'home'; st.rerun()
    components.html(st.session_state.dashboards[active], height=950, scrolling=True)

# ─── ADMIN ────────────────────────────────────────────────────────────────────
def show_admin():
    if st.session_state.role != 'admin':
        st.error("⛔ Access denied."); return
    render_navbar('admin')
    inject_fonts()
    users   = st.session_state.users
    me      = st.session_state.username
    is_main = users.get(me, {}).get('is_main_admin', False)
    pending = st.session_state.pending

    st.markdown(f"""<style>
.aw{{max-width:1060px;margin:0 auto;padding:26px 22px 60px}}
.asec{{background:#0c1018;border:1px solid rgba(255,255,255,.07);border-radius:14px;padding:20px 22px;margin-bottom:16px}}
.asec-t{{font-family:'Syne',sans-serif;font-size:10px;font-weight:700;color:#f7c948;margin-bottom:14px;text-transform:uppercase;letter-spacing:.9px}}
.sg{{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:11px}}
.sc{{background:#111520;border:1px solid rgba(255,255,255,.06);border-radius:12px;padding:14px 16px;position:relative;overflow:hidden}}
.sc::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--c,#4f8ef7)}}
.sv{{font-family:'Syne',sans-serif;font-size:26px;font-weight:800;color:#eceef5;line-height:1}}
.sl{{font-size:10px;color:#4a5068;text-transform:uppercase;letter-spacing:.5px;margin-top:5px}}
.ut-grid{{display:grid;grid-template-columns:1.6fr 0.9fr 0.7fr 2fr;gap:8px;align-items:center}}
.ut-row{{padding:9px 10px;border-bottom:1px solid rgba(255,255,255,.04)}}
.badg{{border-radius:8px;padding:2px 8px;font-size:10px;font-weight:700;display:inline-block}}
.ba{{background:rgba(247,201,72,.1);border:1px solid rgba(247,201,72,.2);color:#f7c948}}
.bm{{background:rgba(180,79,231,.1);border:1px solid rgba(180,79,231,.2);color:#b44fe7}}
.bv{{background:rgba(79,142,247,.1);border:1px solid rgba(79,142,247,.2);color:#4f8ef7}}
.bs{{background:rgba(247,111,79,.1);border:1px solid rgba(247,111,79,.2);color:#f76f4f}}
.bok{{background:rgba(79,206,143,.1);border:1px solid rgba(79,206,143,.2);color:#4fce8f}}
.tok-box{{background:#060910;border:1px solid rgba(79,206,143,.2);border-radius:8px;padding:10px 14px;margin-top:8px;font-family:monospace;font-size:12px;color:#4fce8f;word-break:break-all}}</style>
<div class="aw"><h2 style="font-family:Syne,sans-serif;font-size:18px;font-weight:800;margin-bottom:18px">⚙️ Admin Panel — {'Main Admin' if is_main else 'Admin'}</h2></div>""", unsafe_allow_html=True)

    st.markdown('<div class="aw">', unsafe_allow_html=True)

    # Stats
    total_u  = len(users); active_u = sum(1 for u in users.values() if not u.get('suspended')); admin_u = sum(1 for u in users.values() if u['role']=='admin')
    st.markdown(f'''<div class="asec"><div class="asec-t">📊 System Overview</div><div class="sg">
      <div class="sc" style="--c:#4f8ef7"><div class="sv">{total_u}</div><div class="sl">Total Users</div></div>
      <div class="sc" style="--c:#4fce8f"><div class="sv">{active_u}</div><div class="sl">Active</div></div>
      <div class="sc" style="--c:#f7c948"><div class="sv">{admin_u}</div><div class="sl">Admins</div></div>
      <div class="sc" style="--c:#4fd8f7"><div class="sv">{len(pending)}</div><div class="sl">Pending</div></div>
    </div></div>''', unsafe_allow_html=True)

    # User table
    st.markdown('<div class="asec"><div class="asec-t">👥 User Management</div>', unsafe_allow_html=True)
    for uname, ud in list(users.items()):
        is_self=uname==me; is_susp=ud.get('suspended',False); is_main_u=ud.get('is_main_admin',False); role=ud['role']
        rbadge = '<span class="badg ba">Main Admin</span>' if is_main_u else ('<span class="badg bm">Admin</span>' if role=='admin' else '<span class="badg bv">Viewer</span>')
        sbadge = '<span class="badg bs">Suspended</span>' if is_susp else '<span class="badg bok">Active</span>'
        you_tag = ' <span style="font-size:10px;color:#4fce8f">(you)</span>' if is_self else ''
        st.markdown(f'<div class="ut-row"><b>{ud["name"]}{you_tag}</b> @{uname} &nbsp;{rbadge}&nbsp;{sbadge}</div>', unsafe_allow_html=True)
        if not is_self and not is_main_u:
            a1,a2,a3,a4,_ = st.columns([0.6,0.7,0.6,0.6,2])
            with a1:
                lbl = "▶ Activate" if is_susp else "⏸ Suspend"
                if st.button(lbl, key=f"s_{uname}", use_container_width=True):
                    action = 'activate' if is_susp else 'suspend'
                    if is_main: _apply_action({'action':action,'target':uname,'payload':{},'req_by':me}); st.success(f"✅ Done")
                    else: _queue(action,uname,{},me)
                    st.rerun()
            with a2:
                nr = 'admin' if role=='viewer' else 'viewer'
                if st.button(f"→ {nr.title()}", key=f"r_{uname}", use_container_width=True):
                    if is_main: _apply_action({'action':'change_role','target':uname,'payload':{'new_role':nr},'req_by':me}); st.success(f"✅ Done")
                    else: _queue('change_role',uname,{'new_role':nr},me)
                    st.rerun()
            with a3:
                if st.button("🔑 Reset", key=f"rk_{uname}", use_container_width=True):
                    if is_main:
                        tok=secrets.token_urlsafe(10); st.session_state.users[uname]['reset_token']=tok; st.session_state[f'tok_{uname}']=tok
                    else: _queue('reset_token',uname,{},me)
                    st.rerun()
            with a4:
                if sum(1 for u in users.values() if u['role']=='admin') > 1 or role!='admin':
                    if st.button("🗑 Delete", key=f"d_{uname}", use_container_width=True):
                        st.session_state[f'cdel_{uname}']=True; st.rerun()
        if st.session_state.get(f'tok_{uname}'):
            st.markdown(f'<div class="tok-box">🔑 Reset token for @{uname}: <b>{st.session_state[f"tok_{uname}"]}</b></div>', unsafe_allow_html=True)
            if st.button("✖ Dismiss", key=f"dis_{uname}"): del st.session_state[f'tok_{uname}']; st.rerun()
        if st.session_state.get(f'cdel_{uname}'):
            st.warning(f"Delete {ud['name']}?")
            cy,cn=st.columns(2)
            with cy:
                if st.button("✅ Yes", key=f"cy_{uname}", type="primary", use_container_width=True):
                    if is_main: _apply_action({'action':'delete','target':uname,'payload':{},'req_by':me})
                    else: _queue('delete',uname,{},me)
                    if f'cdel_{uname}' in st.session_state: del st.session_state[f'cdel_{uname}']
                    st.rerun()
            with cn:
                if st.button("✖ Cancel", key=f"cn_{uname}", use_container_width=True): del st.session_state[f'cdel_{uname}']; st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # Add user
    st.markdown('<div class="asec"><div class="asec-t">➕ Add New User</div>', unsafe_allow_html=True)
    c1,c2,c3,c4=st.columns(4)
    nu=c1.text_input("Username",key="nu",placeholder="username"); nn=c2.text_input("Display Name",key="nn",placeholder="Full Name")
    np_=c3.text_input("Password",key="np_",placeholder="password",type="password"); nr2=c4.selectbox("Role",["viewer","admin"],key="nr2")
    if st.button("➕ Add User",key="au",type="primary"):
        if nu and nn and np_:
            ukey=nu.strip().lower()
            if ukey in st.session_state.users: st.warning(f"'{ukey}' exists.")
            else:
                st.session_state.users[ukey]={"hash":_hash(np_),"role":nr2,"name":nn.strip(),"suspended":False,"reset_token":"","is_main_admin":False}
                st.success(f"✅ Added '{ukey}'."); st.rerun()
        else: st.warning("Fill all fields.")
    st.markdown('</div>', unsafe_allow_html=True)

    # Change password
    st.markdown('<div class="asec"><div class="asec-t">🔑 Change Password</div>', unsafe_allow_html=True)
    c1,c2,c3=st.columns(3)
    cpu=c1.selectbox("User",list(users.keys()),key="cpu"); cpn=c2.text_input("New Password",key="cpn",type="password"); cpc=c3.text_input("Confirm",key="cpc",type="password")
    if st.button("🔑 Update",key="cpb",type="primary"):
        if cpn and cpn==cpc: st.session_state.users[cpu]['hash']=_hash(cpn); st.session_state.users[cpu]['reset_token']=''; st.success("✅ Updated.")
        elif cpn!=cpc: st.error("Passwords don't match.")
        else: st.warning("Enter password.")
    st.markdown('</div></div>', unsafe_allow_html=True)

def _queue(action,target,payload,req_by):
    st.session_state.pending.append({'action':action,'target':target,'payload':payload,'req_by':req_by})
    st.info("📨 Request queued for main admin approval.")

def _apply_action(req):
    users=st.session_state.users; target=req['target']; action=req['action']
    if action=='suspend': users[target]['suspended']=True
    elif action=='activate': users[target]['suspended']=False
    elif action=='change_role': users[target]['role']=req['payload']['new_role']
    elif action=='delete':
        if target in users: del users[target]
    elif action=='reset_token':
        tok=secrets.token_urlsafe(10); users[target]['reset_token']=tok; st.session_state[f'tok_{target}']=tok

# ─── ROUTER ───────────────────────────────────────────────────────────────────
if not st.session_state.logged_in:
    show_login()
else:
    pg = st.session_state.page
    if   pg == 'home':      show_home()
    elif pg == 'dashboard': show_dashboard()
    elif pg == 'admin':     show_admin()
    else:                   show_home()
